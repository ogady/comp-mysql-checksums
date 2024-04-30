# comp-mysql-checksums
MySQLサーバー間で特定のDBスキーマの全テーブルのchecksumを比較するツール

## install
```
go install github.com/ogady/comp-mysql-checksums@latest
export GOBIN=~/bin
export PATH=$PATH:$GOBIN
```

## usage

1. `./sample/config.yaml`を参考に`./config.yaml`を作成する

2. 下記コマンドで実行（-pで並列数を指定できる）
```
comp-mysql-checksums -f ./config.yaml -p 4 -s ${Target Database Schema}
```

比較対象のSchemaを別にしたい場合は引数を変える
```
comp-mysql-checksums -f ./config.yaml -p 4 -ss ${Target Database Schema 1} -st ${Target Database Schema 2}
```
