#!bash/sh

make all
cd bench
erlc *.erl
cd -
erl -pa ./ebin -pa ./deps/*/ebin -pa bench -s bench -noinput -noshell