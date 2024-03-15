# amplify-imageproc-runner
Listens to rabbitmq channel, 
downloads referenced media from s3, 
passes media to a process-container, 
saves the output to s3, 
logs all provenance steps, 
sends new message to rabbitmq on a new channel




