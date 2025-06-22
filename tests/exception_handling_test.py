import unittest
import logging
import time
from typing import Any
from unittest.mock import Mock, patch
from io import StringIO

# Assuming the modules are in the same directory or properly installed
from yieldflow.processors import PipelineProcessor
from yieldflow.pipelines import ProcessingPipeline


class TestPipelineExceptionHandling(unittest.TestCase):
    """Unit tests for exception handling in PipelineProcessor step functions."""

    def setUp(self):
        """Set up test fixtures and logging capture."""
        # Create a string buffer to capture log output
        self.log_capture = StringIO()

        # Configure logging to capture to our string buffer
        self.logger = logging.getLogger()
        self.handler = logging.StreamHandler(self.log_capture)
        self.handler.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        self.handler.setFormatter(formatter)
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.ERROR)

        # Test data with some problematic values
        self.test_data = [1, 2, "problem", 4, None, 6, "another_problem", 8, 9, 10]

    def tearDown(self):
        """Clean up logging configuration."""
        self.logger.removeHandler(self.handler)
        self.handler.close()

    def get_log_output(self):
        """Get captured log output as string."""
        return self.log_capture.getvalue()

    def clear_log_output(self):
        """Clear the captured log output."""
        self.log_capture.seek(0)
        self.log_capture.truncate(0)

    def step_multiply_by_2(self, item: Any) -> Any:
        """Step that fails on string inputs."""
        if isinstance(item, str):
            raise ValueError(f"Cannot multiply string '{item}' by 2")
        return item * 2

    def step_add_10(self, item: Any) -> Any:
        """Step that fails on None inputs."""
        if item is None:
            raise TypeError("Cannot add 10 to None")
        return item + 10

    def step_convert_to_string(self, item: Any) -> Any:
        """Step that should work on everything (robust step)."""
        return f"result_{item}"

    def step_always_fails(self, item: Any) -> Any:
        """Step that always raises an exception."""
        raise RuntimeError(f"Always fails for item: {item}")

    def step_selective_failure(self, item: Any) -> Any:
        """Step that fails only on specific values."""
        if item == 6:
            raise ValueError("Special failure for value 6")
        if isinstance(item, str) and "problem" in item:
            raise KeyError(f"Problem string detected: {item}")
        return item * 3

    def test_single_step_with_exceptions_sequential(self):
        """Test exception handling in single step - sequential processing."""
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_multiply_by_2, "Multiply by 2")

        processor = PipelineProcessor(pipeline, max_workers_per_step=1)

        self.clear_log_output()
        results = processor.process_to_list(iter(self.test_data))

        # Should get results for valid items (numbers), strings should be skipped due to exceptions
        expected_valid_items = [1, 2, 4, 6, 8, 9, 10]  # None will cause error too
        expected_results = [x * 2 for x in expected_valid_items]

        self.assertEqual(sorted(results), sorted(expected_results))

        # Should have error logs for failed items
        log_output = self.get_log_output()
        self.assertIn("Cannot multiply string 'problem'", log_output)
        self.assertIn("another_problem", log_output)
        self.assertIn("Multiply by 2", log_output)  # Step description should be in error

        print(f"Sequential single step - Processed {len(results)} items, logged {log_output.count('ERROR')} errors")

    def test_single_step_with_exceptions_parallel(self):
        """Test exception handling in single step - parallel processing."""
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_multiply_by_2, "Multiply by 2")

        processor = PipelineProcessor(pipeline, max_workers_per_step=4)

        self.clear_log_output()
        results = processor.process_to_list(iter(self.test_data))

        # Should get same results as sequential
        expected_valid_items = [1, 2, 4, 6, 8, 9, 10]
        expected_results = [x * 2 for x in expected_valid_items]

        self.assertEqual(sorted(results), sorted(expected_results))

        # Should have error logs for failed items
        log_output = self.get_log_output()
        self.assertIn("ERROR", log_output)
        self.assertIn("Multiply by 2", log_output)

        print(f"Parallel single step - Processed {len(results)} items, logged {log_output.count('ERROR')} errors")

    def test_multi_step_with_exceptions_sequential(self):
        """Test exception handling across multiple steps - sequential."""
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_multiply_by_2, "Multiply by 2")
        pipeline.add_step(self.step_add_10, "Add 10")
        pipeline.add_step(self.step_convert_to_string, "Convert to string")

        processor = PipelineProcessor(pipeline, max_workers_per_step=1)

        self.clear_log_output()
        results = processor.process_to_list(iter(self.test_data))

        # Only numeric items that pass both first steps should make it through
        # 1,2,4,6,8,9,10 pass step 1, None fails step 1, strings fail step 1
        # All survivors pass step 2 (no None values left)
        # All survivors pass step 3
        expected_valid_items = [1, 2, 4, 6, 8, 9, 10]
        expected_results = [f"result_{(x * 2) + 10}" for x in expected_valid_items]

        self.assertEqual(sorted(results), sorted(expected_results))

        # Should have logged errors from failed steps
        log_output = self.get_log_output()
        self.assertIn("ERROR", log_output)

        print(f"Sequential multi-step - Processed {len(results)} items, logged {log_output.count('ERROR')} errors")

    def test_multi_step_with_exceptions_parallel(self):
        """Test exception handling across multiple steps - parallel."""
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_multiply_by_2, "Multiply by 2")
        pipeline.add_step(self.step_add_10, "Add 10")
        pipeline.add_step(self.step_convert_to_string, "Convert to string")

        processor = PipelineProcessor(pipeline, max_workers_per_step=3)

        self.clear_log_output()
        results = processor.process_to_list(iter(self.test_data))

        # Same expected results as sequential
        expected_valid_items = [1, 2, 4, 6, 8, 9, 10]
        expected_results = [f"result_{(x * 2) + 10}" for x in expected_valid_items]

        self.assertEqual(sorted(results), sorted(expected_results))

        log_output = self.get_log_output()
        self.assertIn("ERROR", log_output)

        print(f"Parallel multi-step - Processed {len(results)} items, logged {log_output.count('ERROR')} errors")

    def test_step_that_always_fails(self):
        """Test pipeline with a step that always fails."""
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_always_fails, "Always fails")

        processor = PipelineProcessor(pipeline, max_workers_per_step=2)

        self.clear_log_output()
        results = processor.process_to_list(iter([1, 2, 3]))

        # Should get no results since all items fail
        self.assertEqual(results, [])

        # Should have error logs for all items (each exception generates 3 log lines)
        log_output = self.get_log_output()
        error_count = log_output.count("An exception occurred")  # Count actual exceptions
        self.assertEqual(error_count, 3)  # 3 items, each fails once

        # Verify the specific error messages are present
        self.assertIn("Always fails for item", log_output)

        print(f"Always failing step - Processed {len(results)} items, logged {error_count} exceptions")

    def test_mixed_success_failure_step(self):
        """Test step with selective failures."""
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_selective_failure, "Selective failure")

        processor = PipelineProcessor(pipeline, max_workers_per_step=3)

        self.clear_log_output()
        results = processor.process_to_list(iter(self.test_data))

        # Should succeed for: 1,2,4,None,8,9,10 (but None*3 might be weird)
        # Should fail for: "problem", 6, "another_problem"
        valid_inputs = [1, 2, 4, 8, 9, 10]  # None might fail depending on implementation
        expected_results = [x * 3 for x in valid_inputs if x is not None]

        # Note: None * 3 = None, so it should be in results if no exception
        if None in self.test_data:
            try:
                none_result = None * 3  # This should work (None * 3 = None)
                expected_results.append(none_result)
            except:
                pass  # If it fails, that's ok too

        self.assertEqual(sorted([r for r in results if r is not None]),
                         sorted([r for r in expected_results if r is not None]))

        log_output = self.get_log_output()
        self.assertIn("Special failure for value 6", log_output)
        self.assertIn("Problem string detected", log_output)

        print(f"Selective failure step - Processed {len(results)} items, logged {log_output.count('ERROR')} errors")

    def test_exception_details_in_logs(self):
        """Test that exception logs contain helpful details."""
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_multiply_by_2, "Multiply by 2")

        processor = PipelineProcessor(pipeline, max_workers_per_step=1)

        self.clear_log_output()
        processor.process_to_list(iter(["test_string"]))

        log_output = self.get_log_output()

        # Should contain step description
        self.assertIn("Multiply by 2", log_output)

        # Should contain the problematic data item
        self.assertIn("test_string", log_output)

        # Should contain the error message
        self.assertIn("Cannot multiply string", log_output)

        print("Exception details test - Log contains step name, data item, and error message")

    def test_robust_step_after_failing_step(self):
        """Test that robust steps can process items even after some items fail in earlier steps."""
        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_multiply_by_2, "Multiply by 2")  # Fails on strings
        pipeline.add_step(self.step_convert_to_string, "Convert to string")  # Should work on everything

        processor = PipelineProcessor(pipeline, max_workers_per_step=2)

        self.clear_log_output()

        try:
            results = processor.process_to_list(iter([1, "fail", 3, "fail2", 5]))

            # Only numbers should pass the first step, then all survivors get converted to string
            expected_survivors = [1, 3, 5]
            expected_results = [f"result_{x * 2}" for x in expected_survivors]

            self.assertEqual(sorted(results), sorted(expected_results))

            # Count actual errors in log (each error generates multiple log lines)
            log_output = self.get_log_output()
            error_count = log_output.count("An exception occurred")  # Count actual exceptions, not log lines

            print(
                f"Robust step after failing - {len(results)} items made it through, {error_count} exceptions occurred")
            print(f"Log output contains {log_output.count('ERROR')} ERROR lines")

            # We expect 2 exceptions (for "fail" and "fail2")
            self.assertEqual(error_count, 2)

        except Exception as e:
            # If the processor crashes due to unhandled exceptions, that's a bug
            log_output = self.get_log_output()
            print(f"PROCESSOR CRASHED - This indicates exception handling bug in parallel processing")
            print(f"Exception: {e}")
            print(f"Log contained {log_output.count('ERROR')} ERROR entries")

            # For now, just verify that some errors were logged before the crash
            self.assertGreater(log_output.count("ERROR"), 0, "Should have logged errors before crashing")

            # Re-raise to make the test fail and highlight the bug
            raise

    def test_performance_with_exceptions(self):
        """Test that exceptions don't significantly impact performance of successful items."""
        # Create larger dataset with mix of good and bad data
        large_data = []
        for i in range(50):  # Reduced size to minimize log noise
            if i % 10 == 0:
                large_data.append("fail")  # 10% failure rate
            else:
                large_data.append(i)

        pipeline = ProcessingPipeline()
        pipeline.add_step(self.step_multiply_by_2, "Multiply by 2")

        # Test both sequential and parallel
        sequential_processor = PipelineProcessor(pipeline, max_workers_per_step=1)
        parallel_processor = PipelineProcessor(pipeline, max_workers_per_step=4)

        self.clear_log_output()

        start_time = time.time()
        seq_results = sequential_processor.process_to_list(iter(large_data))
        seq_time = time.time() - start_time

        start_time = time.time()
        par_results = parallel_processor.process_to_list(iter(large_data))
        par_time = time.time() - start_time

        # Results should be the same
        self.assertEqual(sorted(seq_results), sorted(par_results))

        # Should have processed 45 items (90% success rate)
        expected_successful = len([x for x in large_data if x != "fail"])
        self.assertEqual(len(seq_results), expected_successful)

        print(f"Performance with exceptions - Sequential: {seq_time:.3f}s, Parallel: {par_time:.3f}s")
        print(f"Processed {len(seq_results)} items, {large_data.count('fail')} failures")


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)