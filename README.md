# amplify-imageproc-runner
An [AMPLIfy PoC v1](https://github.com/WHOIGit/amplify-imageproc-poc) container that:
1. Asynchronously Listens to rabbitmq channel
2. downloads referenced media from s3
3. passes media to a process-container (image dither api endpoint)
4. saves the output to s3
5. logs all provenance steps
6. sends new message to rabbitmq on a new channel




