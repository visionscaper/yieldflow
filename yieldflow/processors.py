from typing import Any, Callable, List, Iterator, Generator, Optional, Tuple

from concurrent.futures import ThreadPoolExecutor, as_completed

from basics.base import Base
from basics.logging_utils import log_exception

from yieldflow.pipelines import ProcessingPipeline, ProcessStepFunc


ProcessDataThroughPipeLine = Callable[
    [
        Iterator[Any], # Data items iterator
        Iterator[Tuple[str, ProcessStepFunc]] # Iterator over tuples (step_description, step_func)
    ],
    Generator[Any, None, None]
]


class PipelineProcessor(Base):
    """
    A processor that executes data through a ProcessingPipeline with generator-based steps.
    Uses recursive parallel processing for elegant multi-threading.
    """

    def __init__(
        self,
        pipeline: ProcessingPipeline,
        max_workers_per_step: int = None,
        name: Optional[str] = None
    ):
        """
        Initialize the processor with a pipeline.

        Args:
            pipeline: The ProcessingPipeline to use for processing
            max_workers_per_step: Maximum number of threads for parallel processing per step
                If not given, the processing will be executed in a single thread
        """
        super().__init__(pybase_logger_name=name)

        self._pipeline = pipeline or ProcessingPipeline()
        self._max_workers_per_step = max_workers_per_step

    def __call__(self, data: Iterator[Any]) -> Generator[Any, None, None]:
        """
        Process data through the entire pipeline, yielding results as they become available.
        Signature is such that this PipelineProcessor can be used as a ProcessStepFunc in a larger pipeline.

        Args:
            data: The input data to process

        Yields:
            Processed data items from the pipeline
        """
        pipeline_iter = iter(self._pipeline)
        process_stream_func: ProcessDataThroughPipeLine = self._process_parallel \
            if self._max_workers_per_step and self._max_workers_per_step > 1 \
            else self._process_sequential

        yield from process_stream_func(data, pipeline_iter)

    def process_to_list(self, data: Iterator[Any]) -> List[Any]:
        """
        Process data through the pipeline and return results as a list.

        Args:
            data: The input data to process

        Returns:
            List of all processed results
        """
        return list(self(data))

    def _process_sequential(
        self,
        data: Iterator[Any],
        pipeline_iter: Iterator[Tuple[str, ProcessStepFunc]]
    ) -> Generator[Any, None, None]:
        """
        Process data sequentially through the pipeline (recursive).

        Args:
            data: Iterator of data items to process
            pipeline_iter: Iterator over pipeline steps, given by tuples (step_description, step_func)

        Yields:
            Results from the pipeline
        """
        try:
            step_description, step_func = next(pipeline_iter)
        except StopIteration:
            # No more steps, yield the data as final results
            yield from data
            return

        def _apply_step_to_data() -> Generator[Any, None, None]:
            for item in data:
                try:
                    yield step_func(item)
                except Exception as e:
                    log_exception(
                        self._log,
                        f"Failed to process data: \n"
                        f"step: {step_description}\n"
                        f"data item: {item}\n",
                        e
                    )
                    continue

        # Apply current step and recursively process through remaining steps
        yield from self._process_sequential(_apply_step_to_data(), pipeline_iter)

    def _process_parallel(
        self,
        data: Iterator[Any],
        pipeline_iter: Iterator[Tuple[str, ProcessStepFunc]]
    ) -> Generator[Any, None, None]:
        """
        Process data in parallel through the pipeline (recursive).

        Args:
            data: Iterator of data items to process
            pipeline_iter: Iterator over pipeline steps, given by tuples (step_description, step_func)

        Yields:
            Results from the pipeline as they become available
        """
        try:
            step_description, step_func = next(pipeline_iter)
        except StopIteration:
            # No more steps, yield the data as final results
            yield from data
            return

        # Process items in parallel through current step with true streaming
        def _apply_step_to_data() -> Generator[Any, None, None]:
            with ThreadPoolExecutor(max_workers=self._max_workers_per_step) as executor:
                # Submit items as they arrive from the data iterator
                future_map = {executor.submit(step_func, item): item for item in data}

                # Yield results as they complete (streaming)
                for future in as_completed(future_map):
                    item = future_map[future]
                    try:
                        yield future.result()
                    except Exception as e:
                        log_exception(
                            self._log,
                            f"Failed to process data: \n"
                            f"step: {step_description}\n"
                            f"data item: {item}\n",
                            e
                        )
                        continue

        # Recursively process through remaining steps
        yield from self._process_parallel(_apply_step_to_data(), pipeline_iter)
