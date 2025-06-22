# Yieldflow
Simple library for building multi-threaded data processing pipelines, streaming results between steps.

The implementation applies pipeline parallelism: per step processing is parallelized (using ThreadPools) over the input items. 
