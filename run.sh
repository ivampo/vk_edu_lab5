set -eu
cd "$(dirname "$0")"
mkdir -p logs

k() {
    kubectl --context kind-vk-lab5 "$@"
}

apply() {
    k apply -f "k8s/$1.yaml" -l "stage=$2"
}

job() {
    k -n lab5 delete job "simple-$1" --ignore-not-found --wait=true
    apply jobs "$1"
    k -n lab5 wait --for="condition=${2:-Complete}" "job/simple-$1" --timeout=180s
    k -n lab5 get pods -l "job-name=simple-$1" -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,REASON:.status.containerStatuses[0].state.terminated.reason,EXIT:.status.containerStatuses[0].state.terminated.exitCode,START:.status.containerStatuses[0].state.terminated.startedAt,FINISH:.status.containerStatuses[0].state.terminated.finishedAt'
    for pod in $(k -n lab5 get pods -l "job-name=simple-$1" -o name); do
        k -n lab5 logs "$pod" --timestamps
    done
}

case "$1" in
build)
    {
        docker build -t vk-lab5-simple:1 .
        kind load docker-image vk-lab5-simple:1 --name vk-lab5
        k get nodes -o wide
    } > logs/01-cluster.txt 2>&1
    ;;
memory)
    {
        job oom Failed
        job measure
        job tuned
        job single
    } > logs/02-memory.txt 2>&1
    ;;
parallel)
    {
        job split
        job parallel
        job merge
    } >> logs/03-s3-parallel.txt 2>&1
    ;;
scheduling)
    {
        apply cpu cpu-holder
        k -n lab5 wait --for=condition=Ready pod/cpu-holder --timeout=60s
        apply cpu cpu-waiter
        k -n lab5 wait --for=condition=PodScheduled=false pod/cpu-waiter --timeout=60s
        k -n lab5 get pod cpu-waiter -o wide
        k -n lab5 get events --field-selector involvedObject.name=cpu-waiter
        k -n lab5 delete pod cpu-holder
        k -n lab5 wait --for=condition=Ready pod/cpu-waiter --timeout=60s
        k -n lab5 get pod cpu-waiter -o wide
        k -n lab5 delete pod cpu-waiter
    } > logs/04-cpu.txt 2>&1
    {
        apply priorities setup
        apply priorities low
        k -n lab5 wait --for=condition=Ready pod/low --timeout=60s
        k -n lab5 get pod low -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,PRIORITY:.spec.priority,PHASE:.status.phase'
        apply priorities high
        k -n lab5 wait --for=condition=Ready pod/high --timeout=60s
        k -n lab5 get pod high -o 'custom-columns=NAME:.metadata.name,UID:.metadata.uid,PRIORITY:.spec.priority,PHASE:.status.phase'
        k -n lab5 get events --field-selector involvedObject.name=low,reason=Preempted
        k -n lab5 delete pod high
        apply quota setup
        apply quota quota-a
        k -n lab5-quota wait --for=condition=Ready pod/quota-a --timeout=60s
        k -n lab5-quota describe quota compute
        if apply quota quota-b; then
            exit 1
        fi
        k -n lab5-quota delete pod quota-a
        apply quota quota-b
        k -n lab5-quota wait --for=condition=Ready pod/quota-b --timeout=60s
        k -n lab5-quota get pod quota-b -o wide
        k -n lab5-quota delete pod quota-b
    } > logs/05-priority-quota.txt 2>&1
    ;;
*)
    echo 'bash run.sh build | memory | parallel | scheduling'
    exit 1
    ;;
esac
