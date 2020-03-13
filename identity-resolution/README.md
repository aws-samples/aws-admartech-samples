# Identity Resolution Using Amazon Neptune

Identity Resolution is the process of matching human identity across a set of devices used by the same person or a household of persons for the purposes of building a representative identity, or known attributes, for targeted advertising. Included in this repository is a sample solution for building an identity resolution solution using Amazon Neptune, a managed graph database service on AWS.

## Getting Started

This repo includes the following assets:
- A [Jupyter notebook](notebooks/identity-resolution/identity-resolution-sample.ipynb) containing a more thorough explanation of the Identity Resolution use case, the dataset that is being used, the graph data model, and graph queries that are used in deriving identities, audiences, customer journeys, etc.
- A [sample dataset](data/DATA.md) comprised of anonymized cookies, device IDs, and website visits.  It also includes additional manufactured data that enriches the original anonymized dataset to make this more realistic.
- A set of [Python scripts](notebooks/identity-resolution/nepytune) that are used within the Jupyter notebook for executing each of the different use cases and examples.  We're providing the code for these scripts here such that you can extend these for your own benefit.
- A [CloudFormation template](templates/identity-resolution.yml) to launch each of these resources along with the necessary infrastructure.  This template will create an Amazon Neptune database cluster and load the sample dataset into the cluster.  It will also create a SageMaker Jupyter Notebook instance and install the scripts and sample Jupyter notebook to this instance for you to run against the Neptune cluster.

### Architecture

<img src="./images/architecture.png">

### Quickstart

To get started quickly, we have included the following quick-launch link for deploying this sample architecture.

| Region | Stack |
| ---- | ---- |
|US East (N. Virginia) |  [<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://us-east-1.console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/create/review?templateURL=https://s3.amazonaws.com/aws-admartech-samples/identity-resolution/templates/identity-resolution.yml&stackName=Identity-Resolution-Sample) |

Once you have launched the stack, go to the Outputs tab of the root stack and click on the SageMakerNotebook link.  This will bring up the Jupyter notebook console of the SageMaker Jupyter Notebook instance that you created.

<img src="./images/sagemaker-link.png">

Once logged into Jupyter, browse through the Neptune/identity-resolution directories until you see the identity-resolution-sample.ipynb file.  This is the Jupyter notebook containing all of the sample use cases and queries for using Amazon Neptune for Identity Resolution.  Click on the ipynb file.  Additional instructions for each of the use cases are embedded in the Jupyter notebook (ipynb file).

## License Summary

This library is licensed under the MIT-0 License. See the LICENSE file.
