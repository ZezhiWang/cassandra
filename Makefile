all:
	ant

run:
	./bin/cassandra

clean:
	rm -r data
	rm -r logs
	ant clean
