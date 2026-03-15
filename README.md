# system-distrib-docs

Development Sprints Overview

## Sprint 1 - Cluster Skeleton and IPFS Integration

In this sprint, the foundation of the system was established. A Spring Boot API was created, IPFS was installed, and communication between both components was implemented.
When a client uploads a file to the leader node, the file is stored in IPFS. After storage, the system receives a CID (Content Identifier), which uniquely identifies the file within the IPFS network.
Any peer with access to the IPFS network can later request the file using the CID and locally cache a copy.
Additionally, a PubSub communication mechanism was implemented. The leader publishes messages to a specific topic, and all peers subscribed to that topic receive and display those messages in their console.
This proved that the leader could already communicate with the other nodes, although at this stage the system was not yet a fully functional distributed cluster.

## Sprint 2 - Document Vector Versioning and Embeddings

This sprint introduced document list versioning.
Whenever a client uploads a new document, the leader stores it in IPFS and updates the system’s CID list. Instead of replacing the previous list, the leader creates a new version of the document list, preserving the previous one.
At the same time, the leader generates a numerical vector (embedding) representing the semantic content of the document. These embeddings will later enable semantic search.
The leader then sends the following information to all peers via PubSub:
	•	the new version of the CID list
	•	the CID of the new document
	•	the generated embeddings
Each peer receives this information and prepares to validate the proposed version.

## Sprint 3 - Peer Validation and Leader Commit

In this sprint, a distributed validation mechanism was implemented.
When peers receive the proposed new document list version, they verify that the version is consistent with their current state and that no conflicts exist. Each peer then computes a hash of the proposed list, which acts as a signature of the state.
The new version and embeddings are temporarily stored but not yet considered definitive. Each peer sends the computed hash back to the leader.
The leader waits until a majority of responses is received. If the majority of peers return the same hash, the leader assumes consensus and sends a commit message.
Upon receiving the commit, peers promote the temporary version to the official version of the document list and associated data.
This completes the prepare → confirm → commit cycle.

## Sprint 4 - Cluster Communication Consolidation

Sprint 4 focused on consolidating the work initially introduced in Sprint 1.
The PubSub messaging mechanism was validated again to ensure that leader messages correctly reach all subscribed peers.
At this stage, the system still treated leaders and followers as separate roles at startup. Each process was launched with a predefined role, which limited the distributed nature of the cluster.
Between Sprint 4 and Sprint 5, a major refactoring was performed. The system was unified into a single project, where every node starts with the same JAR file. The leader or worker role is now determined dynamically.
This marked the transition from a manually configured setup to a true distributed cluster.

## Sprint 5 - Commit Finalization and Leader Failure Detection

This sprint finalized the commit cycle and introduced leader failure detection.
Once the leader receives confirmation from a majority of peers, it sends the final commit message. Each peer then updates its internal structures and treats the new document list version as the official state.
The leader also begins sending periodic heartbeat messages indicating that it is still alive. Peers track the time since the last heartbeat, and if this exceeds a predefined threshold, they assume the leader has failed.
This introduces the system’s first fail-stop fault handling mechanism, removing the assumption that the leader is always available.

## Sprint 6 - State Recovery and Leader Election (RAFT)

This sprint introduced state recovery and leader election.
The leader periodically publishes state snapshots (the document list) to IPFS and includes the snapshot identifier in its cluster messages. If a peer detects that its state is outdated, it downloads the snapshot from IPFS and replaces its local state.
A simplified implementation of the RAFT consensus algorithm was also introduced. Each node can operate in one of three states:
	•	follower
	•	candidate
	•	leader

If a node stops receiving heartbeats, it assumes the leader has failed and becomes a candidate, requesting votes from the other nodes. If a node receives enough votes, it becomes the new leader and starts sending heartbeats and snapshots.
This allows the cluster to automatically reorganize and elect a new leader when failures occur.

## Sprint 7 - Distributed Search with FAISS (RF2)

This sprint implemented distributed semantic search.
From the user’s perspective, a search endpoint allows submitting a query (sentence or question). The leader registers the request internally, selects an available peer, and sends a work message containing:
	•	the search prompt
	•	the number of requested results (top_k)

The selected peer performs the search using the FAISS index, which has been populated with document embeddings from previous sprints.
The peer returns a list of results containing:
	•	document CIDs
	•	file names
	•	similarity scores

The leader associates the results with the original request and returns them to the client.

For the user, the process appears simple: submit a query and receive relevant documents, while internally the search is executed by a peer in the cluster.

## Sprint 8 - Embedding Pipeline Consolidation with FAISS

Sprint 8 reinforced the work done in Sprint 2 by fully integrating FAISS into the document pipeline.
The full processing pipeline was validated:
	1.	a document enters the system
	2.	an embedding is generated
	3.	the embedding is stored and indexed
	4.	the FAISS index uses these embeddings to answer semantic queries

This sprint confirmed that the complete pipeline of document ingestion, embedding generation, indexing, and semantic search is fully operational.
