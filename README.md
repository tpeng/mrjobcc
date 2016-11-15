MRJobCC
-------

A [MRJob](https://github.com/Yelp/mrjob/tree/master/mrjob) Connected Component implementation of paper proposed in [Connected Components in MapReduce and Beyond](http://dl.acm.org/citation.cfm?id=2670997).
 
Example
-------

for the graph in test.dot: ![graph](https://github.com/tpeng/mrjobcc/raw/master/img/test.png),

getting connected components with networkx:

```
>>> sorted(nx.connected_components(g), key = len, reverse=True)
[{'1', '5', '7', '8', '9'}, {'2', '3', '4', '6'}]
```

with mrjobcc, simply run:

```
python runner.py test.dot
```
will get you same result, but thanks to MRJob, it will just work on Amazon EMR.

Author
------
* Terry Peng <pengtaoo@gmail.com>