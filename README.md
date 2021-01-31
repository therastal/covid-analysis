# Green Team's Question 4

I broke up the green team's question 4 into two separate programs because these methods were working on unrelated datasets, and the methods never referenced each other.

If you're using this as a reference for building unit tests, you'll need to implement a logging solution; otherwise, the output from Spark will drown out the output from any testing module that isn't run last.