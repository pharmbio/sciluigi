import logging
import luigi
import sciluigi as sl
from example3_components import T1, Merge

log = logging.getLogger("sciluigi-interface")


class TestWF(sl.WorkflowTask):

    task = luigi.Parameter()

    def workflow(self):
        t1a = self.new_task("t1a", T1, text="hej_hopp")
        t1b = self.new_task("t1b", T1, text="hopp_hej")

        mrg1 = self.new_task("mrg1", Merge)
        mrg2 = self.new_task("mrg2", Merge)

        # Workflow definition
        mrg1.in_data1 = t1a.out_data1
        mrg1.in_data2 = t1b.out_data1

        mrg2.in_data1 = t1b.out_data1
        mrg2.in_data2 = t1a.out_data1

        for name, instance in locals().items():
            if issubclass(type(instance), sl.Task):
                log.info(
                    f"{name}, task id: {instance.task_id}\n{name}, hash: {instance.__hash__()}"
                )

        return locals()[self.task]


if __name__ == "__main__":
    sl.run_local(main_task_cls=TestWF, cmdline_args=["--task=mrg2"])
