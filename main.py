import csv
import io
import json
import os
import sys
import time
from collections import Counter
from itertools import combinations
from multiprocessing import Pool
from pathlib import Path

import boto3
from botocore.config import Config

BUCKET = 'hh-lake'
SOURCE = 'raw/it_vacancies_2024.csv'
PREFIX = 'results/simple'
OUT = Path('/tmp/result')


def s3():
    return boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'], region_name='us-east-1',
                        config=Config(s3={'addressing_style': 'path'},
                                      request_checksum_calculation='when_required',
                                      response_checksum_validation='when_required'))


def mapper(rows):
    pairs = Counter()
    selected = 0
    for row in rows:
        if row['role'] != 'Программист, разработчик' or 'python' not in row['name'].lower():
            continue
        selected += 1
        skills = sorted({skill.strip(' \t\r\n').lower() for skill in row['key_skills'].split(';')
                         if skill.strip(' \t\r\n')})
        pairs.update(combinations(skills, 2))
    return pairs, selected


def save(pairs, summary, name):
    OUT.mkdir(exist_ok=True)
    path = OUT / 'pairs.csv'
    with path.open('w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['skill_1', 'skill_2', 'vacancies'])
        writer.writerows([*pair, n] for pair, n in sorted(pairs.items()))
    client = s3()
    client.upload_file(str(path), BUCKET, f'{PREFIX}/{name}/pairs.csv')
    summary['unique_pairs'] = len(pairs)
    summary['pair_occurrences'] = sum(pairs.values())
    summary['peak_bytes'] = int(Path('/sys/fs/cgroup/memory.peak').read_text())
    (OUT / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    client.upload_file(str(OUT / 'summary.json'), BUCKET, f'{PREFIX}/{name}/summary.json')
    print(json.dumps(summary, ensure_ascii=False), flush=True)


def count():
    start = time.monotonic()
    name = os.environ['RUN_NAME']
    index = os.getenv('JOB_COMPLETION_INDEX')
    if index is not None:
        name = f'parallel/{index}'
        key = f'parts/part-{int(index):02d}.csv'
    else:
        key = SOURCE
    if os.getenv('SOURCE', 's3') == 'local':
        text = open('/input/it_vacancies_2024.csv', encoding='utf-8-sig', newline='')
    else:
        text = io.TextIOWrapper(s3().get_object(Bucket=BUCKET, Key=key)['Body'],
                                encoding='utf-8-sig', newline='')
    pairs = Counter()
    total = selected = 0
    batch, chunk = [], []
    csv.field_size_limit(10000000)
    with text, Pool(2) as pool:
        for row in csv.DictReader(text):
            total += 1
            chunk.append(row)
            if len(chunk) == 1000:
                batch.append(chunk)
                chunk = []
            if len(batch) == 4:
                for part, n in pool.map(mapper, batch):
                    pairs.update(part)
                    selected += n
                batch = []
        if chunk:
            batch.append(chunk)
        for part, n in pool.map(mapper, batch):
            pairs.update(part)
            selected += n
    summary = {'source_rows': total, 'vacancies': selected,
               'seconds': round(time.monotonic() - start, 3)}
    save(pairs, summary, name)


def split():
    csv.field_size_limit(10000000)
    paths = [Path(f'/tmp/part-{i:02d}.csv') for i in range(4)]
    files = [p.open('w', encoding='utf-8', newline='') for p in paths]
    sizes = [0, 0, 0, 0]
    with open('/input/it_vacancies_2024.csv', encoding='utf-8-sig', newline='') as file:
        reader = csv.DictReader(file)
        writers = [csv.DictWriter(f, reader.fieldnames) for f in files]
        for writer in writers:
            writer.writeheader()
        for i, row in enumerate(reader):
            part = min(i // 60000, 3)
            writers[part].writerow(row)
            sizes[part] += 1
    for file in files:
        file.close()
    client = s3()
    for path in paths:
        client.upload_file(str(path), BUCKET, f'parts/{path.name}')
    print(json.dumps({'part_rows': sizes, 'total': sum(sizes)}), flush=True)


def merge():
    client = s3()
    pairs = Counter()
    total = selected = 0
    for i in range(4):
        prefix = f'{PREFIX}/parallel/{i}'
        body = client.get_object(Bucket=BUCKET, Key=f'{prefix}/pairs.csv')['Body']
        with io.TextIOWrapper(body, encoding='utf-8', newline='') as file:
            for row in csv.DictReader(file):
                pairs[row['skill_1'], row['skill_2']] += int(row['vacancies'])
        summary = json.loads(client.get_object(Bucket=BUCKET, Key=f'{prefix}/summary.json')['Body'].read())
        total += summary['source_rows']
        selected += summary['vacancies']
    save(pairs, {'source_rows': total, 'vacancies': selected}, 'merged')


def download(name):
    folder = Path(__file__).resolve().parent / 'results' / name
    folder.mkdir(parents=True, exist_ok=True)
    for filename in ['pairs.csv', 'summary.json']:
        s3().download_file(BUCKET, f'{PREFIX}/{name}/{filename}', str(folder / filename))


if __name__ == '__main__':
    if len(sys.argv) == 1:
        count()
    elif sys.argv[1] == 'split':
        split()
    elif sys.argv[1] == 'merge':
        merge()
    elif sys.argv[1] == 'download':
        download(sys.argv[2])
