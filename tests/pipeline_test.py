import unittest
import time
from typing import Any

# Assuming the modules are in the same directory or properly installed
from yieldflow.processors import PipelineProcessor
from yieldflow.pipelines import ProcessingPipeline


class TestPipelineProcessor(unittest.TestCase):
    """Unit tests for PipelineProcessor with timing comparisons."""

    def setUp(self):
        """Set up test fixtures."""

        # Test data
        self.test_data = list(range(10))  # [0, 1, 2, ..., 9]

        # Step processing delays (in seconds)
        self.step1_delay = 0.1
        self.step2_delay = 0.15
        self.step3_delay = 0.2

    def step_function_1(self, item: Any) -> Any:
        """First step: multiply by 2 with delay."""
        time.sleep(self.step1_delay)  # Simulate processing time
        return item * 2

    def step_function_2(self, item: Any) -> Any:
        """Second step: add 10 with delay."""
        time.sleep(self.step2_delay)  # Simulate processing time
        return item + 10

    def step_function_3(self, item: Any) -> Any:
        """Third step: convert to string with delay."""
        time.sleep(self.step3_delay)  # Simulate processing time
        return f"result_{item}"

    def direct_execution(self, data):
        """Direct execution: func3(func2(func1(data)))."""
        return [self.step_function_3(
            self.step_function_2(
                self.step_function_1(item)
            )
        ) for item in data]

    def test_pipeline_results_consistency(self):
        """Test that all execution methods produce the same results."""
        # Create pipeline
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_function_1, "Multiply by 2")
        pipeline.add_step(self.step_function_2, "Add 10")
        pipeline.add_step(self.step_function_3, "Convert to string")

        # Single-threaded processor
        single_processor = PipelineProcessor(pipeline, max_workers_per_step=1)

        # Multi-threaded processor
        multi_processor = PipelineProcessor(pipeline, max_workers_per_step=4)

        # Execute all approaches
        single_result = single_processor.process_to_list(iter(self.test_data))
        multi_result = multi_processor.process_to_list(iter(self.test_data))
        direct_result = self.direct_execution(self.test_data)

        # Verify results are identical
        self.assertEqual(sorted(single_result), sorted(direct_result))
        self.assertEqual(sorted(multi_result), sorted(direct_result))

        # Verify expected transformations
        expected = [f"result_{(x * 2) + 10}" for x in self.test_data]
        self.assertEqual(sorted(direct_result), sorted(expected))

    def test_execution_timing_comparison(self):
        """Test and compare execution times between different approaches."""
        # Create pipeline
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_function_1, "Multiply by 2")
        pipeline.add_step(self.step_function_2, "Add 10")
        pipeline.add_step(self.step_function_3, "Convert to string")

        # Processors
        single_processor = PipelineProcessor(pipeline, max_workers_per_step=1)
        multi_processor = PipelineProcessor(pipeline, max_workers_per_step=4)

        # Time direct execution
        print("Starting direct execution timing test...")
        start_time = time.time()
        direct_result = self.direct_execution(self.test_data)
        direct_time = time.time() - start_time
        print(f"Direct execution completed in {direct_time:.3f} seconds")

        # Time single-threaded execution
        print("Starting single-threaded pipeline execution timing test...")
        start_time = time.time()
        single_result = single_processor.process_to_list(iter(self.test_data))
        single_time = time.time() - start_time
        print(f"Single-threaded execution completed in {single_time:.3f} seconds")

        # Time multi-threaded execution
        print("Starting multi-threaded pipeline execution timing test...")
        start_time = time.time()
        multi_result = multi_processor.process_to_list(iter(self.test_data))
        multi_time = time.time() - start_time
        print(f"Multi-threaded execution completed in {multi_time:.3f} seconds")

        # Log timing comparison (using print since unittest suppresses logger output)
        print(f"\n=== TIMING COMPARISON ===")
        print(f"Direct execution:        {direct_time:.3f}s")
        print(f"Single-threaded:         {single_time:.3f}s")
        print(f"Multi-threaded:          {multi_time:.3f}s")
        print(f"Speedup (multi vs single): {single_time / multi_time:.2f}x")
        print(f"Speedup (multi vs direct): {direct_time / multi_time:.2f}x")

        # Also log for any log handlers that might be configured
        print(
            f"Timing results - Direct: {direct_time:.3f}s, Single: {single_time:.3f}s, Multi: {multi_time:.3f}s")

        # Expected minimum processing time calculations
        total_delay_per_item = self.step1_delay + self.step2_delay + self.step3_delay
        expected_direct_time = len(self.test_data) * total_delay_per_item
        expected_single_time = expected_direct_time  # Should be similar

        # Multi-threaded should be faster (though exact speedup depends on overhead)
        print(f"Expected direct time: ~{expected_direct_time:.3f}s")

        # Verify results are consistent
        self.assertEqual(sorted(direct_result), sorted(single_result))
        self.assertEqual(sorted(single_result), sorted(multi_result))

        # Multi-threaded should generally be faster than single-threaded
        # (allowing some tolerance for thread overhead and system variability)
        self.assertLess(multi_time, single_time * 1.2,
                        "Multi-threaded execution should be faster than single-threaded")

    def test_pipeline_with_different_worker_counts(self):
        """Test pipeline performance with different worker counts."""
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_function_1, "Multiply by 2")
        pipeline.add_step(self.step_function_2, "Add 10")
        pipeline.add_step(self.step_function_3, "Convert to string")

        worker_counts = [1, 2, 4, 8]
        times = {}

        for workers in worker_counts:
            processor = PipelineProcessor(pipeline, max_workers_per_step=workers)

            start_time = time.time()
            result = processor.process_to_list(iter(self.test_data))
            execution_time = time.time() - start_time
            times[workers] = execution_time

            print(f"Execution with {workers} workers: {execution_time:.3f}s")

            # Verify result consistency
            expected = [f"result_{(x * 2) + 10}" for x in self.test_data]
            self.assertEqual(sorted(result), sorted(expected))

        print(f"\n=== WORKER COUNT COMPARISON ===")
        for workers, exec_time in times.items():
            speedup = times[1] / exec_time if workers > 1 else 1.0
            print(f"{workers} workers: {exec_time:.3f}s (speedup: {speedup:.2f}x)")

        print(f"Worker count comparison completed")

    def test_empty_pipeline(self):
        """Test processor with empty pipeline."""
        empty_pipeline = ProcessingPipeline()
        processor = PipelineProcessor(empty_pipeline)

        result = processor.process_to_list(iter(self.test_data))
        self.assertEqual(result, self.test_data)

    def test_single_step_pipeline(self):
        """Test processor with single step."""
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_function_1, "Multiply by 2")

        processor = PipelineProcessor(pipeline, max_workers_per_step=4)
        result = processor.process_to_list(iter(self.test_data))

        expected = [x * 2 for x in self.test_data]
        self.assertEqual(sorted(result), sorted(expected))

    def test_generator_based_processing(self):
        """Test that the processor works with generators (streaming)."""
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_function_1, "Multiply by 2")
        pipeline.add_step(self.step_function_2, "Add 10")

        processor = PipelineProcessor(pipeline, max_workers_per_step=2)

        # Test with generator input
        def data_generator():
            for i in range(5):
                yield i

        results = list(processor(data_generator()))

        expected = [(x * 2) + 10 for x in range(5)]
        self.assertEqual(sorted(results), sorted(expected))


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)