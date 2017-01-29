from itertools import islice, chain

def chunks(iterable, size=10):
	iterator = iter(iterable)
	for first in iterator: yield chain([first], islice(iterator, size - 1))
