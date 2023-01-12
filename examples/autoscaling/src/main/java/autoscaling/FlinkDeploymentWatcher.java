package autoscaling;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeploymentList;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/** Autoscaling Example Watcher. */
public class FlinkDeploymentWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkDeploymentWatcher.class);

    public static void main(String[] args) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        KubernetesClient k8sClient = new DefaultKubernetesClient();
        SharedInformerFactory sharedInformerFactory = k8sClient.informers();
        SharedIndexInformer<FlinkDeployment> podInformer =
                sharedInformerFactory.sharedIndexInformerFor(FlinkDeployment.class, 10 * 1000L);
        podInformer.addEventHandler(
                new ResourceEventHandler<>() {

                    @Override
                    public void onAdd(FlinkDeployment obj) {
                        // do nothing
                    }

                    @Override
                    public void onUpdate(FlinkDeployment oldObj, FlinkDeployment newObj) {
                        FlinkDeploymentStatus flinkDeploymentStatus = newObj.getStatus();
                        boolean isJobFail =
                                StringUtils.isNoneBlank(flinkDeploymentStatus.getError());
                        JobStatus jobStatus =
                                Optional.ofNullable(flinkDeploymentStatus.getJobStatus())
                                        .orElse(new JobStatus());
                        System.out.println("Job is " + jobStatus);
                        if (isJobFail
                                || isJobSuccessful(
                                        newObj.getSpec().getFlinkConfiguration(), jobStatus)) {
                            System.out.println("Flink Deployment Status: " + flinkDeploymentStatus);
                            System.out.printf(
                                    "get %s status: %s\n",
                                    newObj.getMetadata().getName(), jobStatus.getState());
                            latch.countDown();
                        }
                    }

                    private boolean isJobSuccessful(
                            Map<String, String> flinkConfiguration, JobStatus jobStatus) {
                        boolean isSuccess = "FINISHED".equalsIgnoreCase(jobStatus.getState());
                        if (isSuccess
                                && flinkConfiguration.containsKey(
                                        "pipeline.finished-grace-seconds")) {
                            Instant jobFinishedTime =
                                    Instant.ofEpochMilli(Long.parseLong(jobStatus.getUpdateTime()));
                            isSuccess =
                                    Duration.between(jobFinishedTime, Instant.now()).toSeconds()
                                            > Integer.parseInt(
                                                    flinkConfiguration.get(
                                                            "pipeline.finished-grace-seconds"));
                            if (isSuccess) {
                                System.out.println(
                                        "Application is completed successfully " + jobStatus);
                            }
                        }
                        return isSuccess;
                    }

                    @Override
                    public void onDelete(FlinkDeployment obj, boolean deletedFinalStateUnknown) {
                        if (latch.getCount() == 1) {
                            latch.countDown();
                        }
                    }
                });
        LOG.info("Starting flinkDeployment informer");
        sharedInformerFactory.startAllRegisteredInformers();
        latch.await();
        podInformer.close();
        deleteCRD(k8sClient);
        System.out.println("Watcher exited");
    }

    private static void deleteCRD(KubernetesClient k8sClient) {
        try {
            MixedOperation<FlinkDeployment, FlinkDeploymentList, Resource<FlinkDeployment>> client =
                    k8sClient.resources(FlinkDeployment.class, FlinkDeploymentList.class);
            Resource<FlinkDeployment> deployment =
                    client.inNamespace("default").withName("autoscaling-example");
            System.out.println(
                    "Deleting flinkDeployment CDR: " + deployment.get().getMetadata().getName());
            deployment.delete();
            System.out.println(
                    "Deleted flinkDeployment CDR: " + deployment.get().getMetadata().getName());
        } catch (Exception e) {
            LOG.error("fail to delete job: ", e);
            throw new RuntimeException("fail to delete job");
        }
    }
}
