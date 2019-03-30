all:
	ant clean
	ant

run:
	./bin/cassandra

clean:
	rm -r data
	rm -r logs
