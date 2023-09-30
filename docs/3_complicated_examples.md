# Complicated examples
Some streams are more complicated that others.
Usually a stream is like
```
start -> apply some functions -> end
```

But sometimes you need to do more complicated things where you want to
add the processed items back to the start of the stream.
```
start -> apply some functions -> add more things to the start -> repeat
```

## Web crawler
A web crawler is a good example of this.
You can easily write a concurrent web crawler with grugstream. 
Here's an example of crawling from one website recursively for 1000 links:

```python
{!docs/crawler.py!}
```
