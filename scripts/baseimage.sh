sudo apt-get update
sudo apt-get install -y libsodium23 libsodium-dev pkg-config

#peer
go install -tags -ldflags="-X github.com/hyperledger/fabric/common/metadata.Version=1.4.8 -X github.com/hyperledger/fabric/common/metadata.CommitSHA=de606c504 -X github.com/hyperledger/fabric/common/metadata.BaseVersion=0.4.21 -X github.com/hyperledger/fabric/common/metadata.BaseDockerLabel=org.hyperledger.fabric -X github.com/hyperledger/fabric/common/metadata.DockerNamespace=hyperledger -X github.com/hyperledger/fabric/common/metadata.BaseDockerNamespace=hyperledger" github.com/hyperledger/fabric/peer
#orderer
go install -tags  -ldflags="-X github.com/hyperledger/fabric/common/metadata.Version=1.4.8 -X github.com/hyperledger/fabric/common/metadata.CommitSHA=de606c504 -X github.com/hyperledger/fabric/common/metadata.BaseVersion=0.4.21 -X github.com/hyperledger/fabric/common/metadata.BaseDockerLabel=org.hyperledger.fabric -X github.com/hyperledger/fabric/common/metadata.DockerNamespace=hyperledger -X github.com/hyperledger/fabric/common/metadata.BaseDockerNamespace=hyperledger" github.com/hyperledger/fabric/orderer