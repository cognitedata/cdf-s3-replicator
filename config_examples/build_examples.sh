#! /bin/bash

for file in `ls *.yaml`; do

	if [[ $file == "local_config_example.yaml" ]]; then
		continue
	fi

	echo Making example-$file

	cat local_config_example.yaml > example-$file
	cat $file >> example-$file
done
