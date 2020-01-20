#!/bin/bash

go_adder="$HOME/devel/go-ipfs/cmd/ipfs/ipfs"
js_adder="jsipfs"

for use_trickle in false true ; do \
	for use_rawleaves in false true ; do \
		for use_chunker in \
				size-65535 \
				size-262144 \
				size-1048576 \
				buzhash \
				rabin \
				rabin-262141 \
				rabin-262144-524288-1048576 \
				rabin-128-65535-524288 \
			; do \
			for inline_limit in 0 32 34 512 ; do \
				for cidver in 0 1 ; do \
					for data_fn in ../testdata/*.zst ; do \
						for use_implementation in go js ; do \
							[[ "$use_implementation" == "js" ]] && ( [[ "$inline_limit" != "0" ]] || [[ "$use_chunker" == "buzhash" ]] || [[ "$cidver" == "0" ]] ) && continue

							adder_varname="${use_implementation}_adder"

							wrapper=()
							core_opts=()
							add_opts=()

							if [[ "$cidver" == "0" ]] ; then
								core_opts+=( "--upgrade-cidv0-in-output=true" )
								add_opts+=( "--cid-version=0" )
							else
								add_opts+=( "--cid-version=1" )
							fi

							if [[ "$inline_limit" != "0" ]] ; then
								add_opts+=( "--inline=true --inline-limit=$inline_limit" )
							fi

							if [[ $data_fn =~ zero ]] ; then
								wrapper+=( timeout 10 )
							fi

							resultCID="$(zstd -qdck $data_fn | ${wrapper[@]} ${!adder_varname} ${core_opts[@]} add -n --quiet --chunker=$use_chunker --trickle=$use_trickle --raw-leaves=$use_rawleaves ${add_opts[@]})"
							echo -e "Data:$data_fn\tImpl:$use_implementation\tTrickle:$use_trickle\tRawLeaves:$use_rawleaves\tInlining:$inline_limit\tCidVer:$cidver\tChunker:$use_chunker\tCmd:${core_opts[@]} add --chunker=$use_chunker --trickle=$use_trickle --raw-leaves=$use_rawleaves ${add_opts[@]}\tCID:$resultCID"

						done
					done
				done
			done
		done
	done
done
