#!/usr/bin/env bash
# Print the Raft members' gRPC addresses (comma separated) once all three
# have published them under $1/addr.
RUN=${1:?run dir}
out=""
for m in meta-0 meta-1 meta-2; do
  while [ ! -s "$RUN/addr/$m" ]; do [ -e "$RUN/stop" ] && exit 1; sleep 1; done
  read -r a _ g <"$RUN/addr/$m"
  out="${out:+$out,}$a:$g"
done
echo "$out"
