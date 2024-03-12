#/bin/bash
# delete template1/2/3 files except for x items

read -p "File path of the directory to clean: " path
read -p "How many files to keep: " keep
keep=$((keep+1))

ls -p $path | sort -R | grep -v '/$' | grep '.*template1.*\.graph' | tail -n +$keep | xargs -I {} rm -- $path/{}
ls -p $path | sort -R | grep -v '/$' | grep '.*template2.*\.graph' | tail -n +$keep | xargs -I {} rm -- $path/{}
ls -p $path | sort -R | grep -v '/$' | grep '.*template3.*\.graph' | tail -n +$keep | xargs -I {} rm -- $path/{}

