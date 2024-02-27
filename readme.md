Yet another data project with dbt

Step by step:

- [install minikube](https://minikube.sigs.k8s.io/docs/start/)
- [install kubectl](https://kubernetes.io/pt-br/docs/tasks/tools/install-kubectl-linux/)
- [install k9s](https://kubernetes.io/pt-br/docs/tasks/tools/install-kubectl-linux/)
- [install helm](https://helm.sh/docs/intro/install/)

minio:
helm repo add bitnami https://charts.bitnami.com/bitnami
helm pull bitnami/minio --untar

trino:
helm repo add trino https://trinodb.github.io/charts
helm pull trino/trino --untar