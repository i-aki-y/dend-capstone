class Scripts:

    launch_cluster_command = ("""
    aws emr create-cluster --applications Name=Hadoop Name=Hive Name=JupyterHub Name=Spark \
    --ec2-attributes KeyName={{ params.key_name }},InstanceProfile=EMR_EC2_DefaultRole,SubnetId={{ params.subnet_id }} \
    --release-label {{ params.release_label }} \
    --log-uri {{ params.log_url }} \
    --instance-groups InstanceCount=2,InstanceGroupType=CORE,InstanceType=m5.xlarge,InstanceCount=1,InstanceGroupType=MASTER,InstanceType=m5.xlarge \
    --configurations '[{"Classification":"spark-env","Properties":{},"Configurations":[{"Classification":"export","Properties":{"PYSPARK_PYTHON":"/usr/bin/python3"}}]},{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]' \
    --auto-scaling-role EMR_AutoScaling_DefaultRole \
    --bootstrap-actions Path={{ params.bootstrap_action }},Name=CustomAction \
    --ebs-root-volume-size 10 --service-role EMR_DefaultRole \
    --enable-debugging --name 'mycluster' \
    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
    --region {{ params.region }} | jq .ClusterId | sed 's/"//g'
    """)

    wait_cluster_setup_command = ("""
    while true; do
       resp="`aws --region {{params.region}} emr describe-cluster --cluster-id {{ ti.xcom_pull(task_ids='launch_cluster') }}`"
       echo "Starting..."
       if [ "`echo \"$resp\" | grep 'WAITING'`" ]; then
           echo "Cluster is up. Continuing."
           break
       fi
       sleep {{ params.pollsecond }}
    done
    """)

    wait_cluster_setup_command2 = ("""
    aws --region {{ params.region }} emr wait cluster-running --cluster-id {{ ti.xcom_pull(task_ids='launch_cluster') }}
    """)

    wait_step_command = ("""
    while true; do
       resp="`aws --region {{params.region}} emr describe-cluster --cluster-id {{ ti.xcom_pull(task_ids='launch_cluster') }}`"
       echo "Starting..."
       if [ "`echo \"$resp\" | grep 'WAITING'`" ]; then
           echo "Cluster is up. Continuing."
           break
       fi
       sleep {{ params.pollsecond }}
    done
    """)

    wait_step_command2 = ("""
    aws --region {{ params.region }} emr wait step-complete \
    --cluster-id {{ ti.xcom_pull(task_ids='launch_cluster') }} \
    --step-id {{ ti.xcom_pull(task_ids='run_etl_spark_job') }}
    """)

    spark_merge_one_day_step_command = ("""
    aws --region {{ params.region }} emr add-steps \
    --cluster-id {{ ti.xcom_pull(task_ids='launch_cluster') }} \
    --steps Type=spark,Name=TestJob,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,{{ params.script_path }},--bucket,{{ params.bucket }},--tmdb_dir,{{ params.tmdb_dir }},--update_prefix,{{ params.update_prefix }}{{ execution_date.strftime("%Y-%m-%d") }}],ActionOnFailure=CONTINUE | jq -r .StepIds[]
    """)

    spark_merge_all_step_command = ("""
    aws --region {{ params.region }} emr add-steps \
    --cluster-id {{ ti.xcom_pull(task_ids='launch_cluster') }} \
    --steps Type=spark,Name=TestJob,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,{{ params.script_path }},--bucket,{{ params.bucket }},--tmdb_dir,{{ params.tmdb_dir }},--update_prefix,{{ params.update_prefix }}],ActionOnFailure=CONTINUE | jq -r .StepIds[]
    """)


    spark_etl_step_command = ("""
    aws --region {{ params.region }} emr add-steps \
    --cluster-id {{ ti.xcom_pull(task_ids='launch_cluster') }} \
    --steps Type=spark,Name=TestJob,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,{{ params.script_path }},--bucket,{{ params.bucket }},--tmdb_dir,{{ params.tmdb_dir }},--ml_dir,{{ params.ml_dir }},--output_dir,{{ params.output_dir }}],ActionOnFailure=CONTINUE | jq -r .StepIds[]
    """)

    spark_check_data_count_command = ("""
    aws --region {{ params.region }} emr add-steps \
    --cluster-id {{ ti.xcom_pull(task_ids='launch_cluster') }} \
    --steps Type=spark,Name=TestJob,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,{{ params.script_path }},--bucket,{{ params.bucket }},--output_dir,{{ params.output_dir }},--target_name,{{ params.target_name }}],ActionOnFailure=CONTINUE | jq -r .StepIds[]
    """)

    spark_check_movie_mapping_command = ("""
    aws --region {{ params.region }} emr add-steps \
    --cluster-id {{ ti.xcom_pull(task_ids='launch_cluster') }} \
    --steps Type=spark,Name=TestJob,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,{{ params.script_path }},--bucket,{{ params.bucket }},--ml_dir,{{ params.ml_dir }},--output_dir,{{ params.output_dir }}],ActionOnFailure=CONTINUE | jq -r .StepIds[]
    """)

    terminate_cluster_command = ("""
    aws --region {{ params.region }} emr terminate-clusters --cluster-ids {{ ti.xcom_pull(task_ids='launch_cluster') }}
    """)
