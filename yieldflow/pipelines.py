from typing import Any, Callable, List, Iterator, Generator, Optional, Dict, Tuple

from basics.base import Base


ProcessStepFunc = Callable[[Any], Any]


class ProcessingPipeline(Base):
    """
    A pipeline data structure that holds generator-based processing steps.
    Each step takes an object and yields one or more objects.
    """
    
    def __init__(
        self,
        step_func_list: Optional[List[ProcessStepFunc]] = None,
        step_func_map: Optional[Dict[str, ProcessStepFunc]] = None,
        name: Optional[str] = None
    ):
        """
        Initialize the pipeline with optional processing steps.
        
        Args:
            step_func_list: List of ProcessStepFunc's to define the pipeline processing steps.
                The name of each step will be generated as "Step 0", "Step 1", etc.
            step_func_map: Dictionary mapping step function names to ProcessStepFunc's that define the
                pipeline processing steps. The order at which the functions are added is used as processing order.
        """
        super().__init__(pybase_logger_name=name)

        if step_func_list is not None and step_func_map is not None:
            raise ValueError("Either provide step_func_list or step_func_map, not both.")

        if step_func_list is not None:
            step_func_map = {
                f"Step {idx}": step_func for idx, step_func in enumerate(step_func_list)
            }


        self._step_func_map = step_func_map or {}
    
    def add_step(
        self,
        step_func: ProcessStepFunc,
        step_description: Optional[str] = None
    ) -> 'ProcessingPipeline':
        """
        Add a processing step to the end of the pipeline.
        
        Args:
            step: A callable that takes any object and yields processed objects,
            step_description: An optional description of the step to add, if not provided a
                unique "Step X" is generated.
            
        Returns:
            Self for method chaining
        """
        if step_description is None:
            step_descriptions = list(self._step_func_map.keys())
            step_idx = len(step_descriptions)
            while True:
                step_description = f"Step {step_idx}"
                if step_description not in step_descriptions:
                    break

        self._log.debug(f"Adding processing step '{step_description}' to pipeline")

        self._step_func_map[step_description] = step_func

        return self

    def __len__(self) -> int:
        """Return the number of steps in the pipeline."""
        return len(self._step_func_map)
    
    def __iter__(self) -> Iterator[Tuple[str, ProcessStepFunc]]:
        """Iterate over the processing steps."""
        return iter(self._step_func_map.items())
