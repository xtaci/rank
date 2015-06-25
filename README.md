# Ranking Service
[![Build Status](https://travis-ci.org/GameGophers/rank.svg)](https://travis-ci.org/GameGophers/rank)

Ranking based on id & score, ranking are grouped by name, and it has persistence with boltdb

make sure directory /data is writable for VOLUME, boltdb will persist the ranking data file into that directory

# environment variables
NSQD_HOST: eg : http://172.17.42.1:4151
