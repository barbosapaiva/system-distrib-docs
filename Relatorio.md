
# Sistemas Distribuídos

#### Sprint 1 - Esqueleto do cluster e ponte com IPFS

Neste sprint tratei de pôr a base a funcionar, API em Spring Boot instlaar o IPFS e coloca-los a comunicar. O cliente envia um ficheiro para o “líder”, este ficheiro é guardado no IPFS e o sistema recebe de volta um CID, que é o identificador único do ficheiro. A partir desse momento, qualquer peer que tenha acesso à rede IPFS consegue pedir esse ficheiro pelo CID e guardar uma cópia em cache.
Além disso, implementei o envio de mensagens pelo líder para um tópico PubSub. Todos os peers que estão subscritos nesse tópico conseguem ver essas mensagens na consola. Isto prova que já existe uma forma simples de o líder “falar” com o resto do cluster, que no momento ainda não era um real cluster.


#### Sprint 2 - Nova versão do vetor de documentos e embeddings

Quando o cliente envia um novo documento, o líder não só o guarda no IPFS como também atualiza a “lista" de documentos do sistema. Em termos simples, o líder pega na lista de CIDs que já existem, adiciona o novo CID e cria uma nova versão dessa lista, sem ainda substituir a versão anterior.
Ao mesmo tempo, o líder gera um vetor numérico que representa o conteúdo do texto (embeddings), isto é o que vai permitir fazer pesquisa semântica mais tarde. Depois, o líder envia para os outros peers a nova versão da lista de CIDs, o CID do documento e estes embeddings através do PubSub. Cada peer recebe esta informação e fica preparado para, no futuro, confirmar esta nova versão.

#### Sprint 3 - Confirmação da versão pelos peers e commit do líder

Dar “voz” aos peers. Quando recebem a proposta de nova versão do vetor de documentos, eles verificam se a versão faz sentido para eles, se não há conflito, e calculam uma espécie de “assinatura” dessa lista, uma hash. Guardam temporariamente esta nova versão e os embeddings, mas ainda não a assumem como definitiva.
Depois, cada peer responde ao líder com a hash que calculou. O líder espera receber respostas suficientes para ter uma maioria. Se a maioria dos peers devolve a mesma hash, o líder assume que todos estão alinhados e envia um commit. A partir daí, os peers promovem essa versão temporária a versão oficial da lista de documentos e dos dados associados. É aqui que se fecha o ciclo prepare - confirmação - commit.

#### Sprint 4 - Recuperação do Sprint 1

O Sprint 4 foi usado para recuperar e consolidar o que estava definido no Sprint 1. 
Também validei na prática o envio de mensagens do líder para o resto do "cluster" via PubSub, mostrando que a mensagem chega a todos os peers que estão subscritos no tópico configurado. 
Até aqui, na prática, eu ainda pensava em líder e followers quase como projetos separados: um processo era arrancado já “marcado” como líder, os outros como followers. Isso limitava muito o lado distribuído, não havia forma do cluster reagir sozinho à queda do líder.
Entre o Sprint 4 e o Sprint 5 refatorei tudo para um único projeto. Agora todos os nós arrancam com o mesmo JAR e o papel de líder ou worker é decidido. Ou seja, passei de uma solução “manual” para um cluster de verdade.
Foi, no fundo, o “fechar” em condições aquilo que tinha ficado em aberto.

#### Sprint 5 - Commit definitivo e deteção de falhas do líder

Fechar o ciclo do commit nos peers e começar a tratar falhas do tipo fail-stop. Depois de o líder receber a maioria de confirmações dos peers, envia o commit final e cada peer passa a considerar a nova versão da lista de documentos como oficial, atualizando também as estruturas em memória que vão ser usadas para pesquisa.
Ao mesmo tempo, o líder passa a enviar mensagens periódicas para o cluster, uma espécie de “estou vivo”. Estas mensagens funcionam como heartbeats. Os peers medem o tempo desde o último heartbeat e, se esse tempo ultrapassar um certo limite, assumem que o líder caiu. Isto é o início da gestão de falhas, o sistema deixa de depender da suposição “o líder nunca morre”.


#### Sprint 6 - Recuperação de estado e eleição de novo líder (RAFT)

Tratei da recuperação e da eleição de líder. Por um lado, o líder passa a publicar snapshots do estado (a lista de documentos) no IPFS e a anunciar o identificador desse snapshot nas mensagens que envia. Se um peer perceber que a sua versão está atrasada em relação à do líder, descarrega esse snapshot do IPFS e substitui o ficheiro local, ficando rapidamente em linha com o estado atual.
Por outro lado, implementei uma versão simplificada de RAFT, cada nó pode estar em estado seguidor, candidato ou líder. Se um peer deixa de receber heartbeats durante algum tempo, assume que o líder morreu e passa a candidato, pedindo votos aos outros. Quando um nó obtém votos suficientes, assume o papel de novo líder e começa ele a enviar heartbeats e snapshots. O efeito prático é, se o líder cair, passado algum tempo o cluster reorganiza-se e elege automaticamente um novo líder.


#### Sprint 7 - Pesquisa distribuída com FAISS (RF2)

Coloquei a pesquisa distribuída a funcionar. Do ponto de vista do utilizador, há um endpoint de pesquisa onde este envia uma frase ou pergunta. O líder regista internamente esse pedido, escolhe um peer que esteja disponível e envia-lhe uma mensagem de trabalho com a prompt e o número de resultados pretendidos (top_k).
O peer que recebe o pedido usa o índice FAISS (que foi sendo alimentado com os embeddings dos documentos nos sprints anteriores) para encontrar os documentos mais parecidos com a frase de entrada. Depois envia de volta para o líder uma lista com os CIDs, nomes e scores desses documentos. O líder associa estes resultados ao pedido original e devolve-os ao cliente. O utilizador vê apenas “faço uma pesquisa e recebo documentos relevantes”, mas por baixo a pesquisa é feita por um peer do cluster.

#### Sprint 8 - Recuperação do Sprint 2 com FAISS

O Sprint 8 foi usado para recuperar e reforçar o Sprint 2, desta vez já com FAISS no circuito. A ideia aqui foi mostrar claramente que o pipeline completo de embeddings está a funcionar, quando entra um documento, é gerado um vetor numérico, esse vetor é guardado e posteriormente utilizado pelo FAISS para responder a pesquisas.
