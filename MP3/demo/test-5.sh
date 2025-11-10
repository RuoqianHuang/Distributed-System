#!/bin/bash

FILENAME="test-5"

./bin/client create $FILENAME /etc/hostname
./bin/client append $FILENAME /etc/hostname


# Change /etc/hostname to other filename
./bin/client multiappend $FILENAME \
    fa25-cs425-b601.cs.illinois.edu /etc/hostname \
    fa25-cs425-b602.cs.illinois.edu /etc/hostname \
    fa25-cs425-b603.cs.illinois.edu /etc/hostname \
    fa25-cs425-b604.cs.illinois.edu /etc/hostname \
    fa25-cs425-b605.cs.illinois.edu /etc/hostname \
    fa25-cs425-b606.cs.illinois.edu /etc/hostname \
    fa25-cs425-b607.cs.illinois.edu /etc/hostname \
    fa25-cs425-b608.cs.illinois.edu /etc/hostname \
    fa25-cs425-b609.cs.illinois.edu /etc/hostname \
    fa25-cs425-b610.cs.illinois.edu /etc/hostname \


rm -f append.result

sleep 2

./bin/client get $FILENAME append.result


cat append.result