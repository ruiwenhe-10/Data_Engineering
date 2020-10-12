gcloud dataproc clusters create runscala --bucket ruiwen_dataproc \
--region us-west1 --subnet default --zone us-west1-b \
--master-machine-type n1-standard-4 --master-boot-disk-size 500 --num-workers 2 \
--worker-machine-type n1-standard-4 --worker-boot-disk-size 500 --image-version 1.3-debian10 \
--project utopian-pact-288603

gcloud dataproc jobs submit spark --cluster runscala --region us-west1 --jar gs://ruiwen_test_storage/bigdata/bigdata-stackoverflow.jar

Y | gcloud dataproc clusters delete runscala