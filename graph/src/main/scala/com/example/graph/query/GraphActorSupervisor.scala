package com.example.graph.query

import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity.{EdgeDirection, EdgeProperties, EdgeType, GraphNodeCommand, GraphNodeCommandReply, NodeId, TargetNodeId}
import com.example.graph.http.Requests.QueryReq
import com.example.graph.query.GraphQueryActor.{GraphQuery, GraphQueryReply}
import com.example.graph.query.GraphActorSupervisor.{GraphQuerySupervisorCommand, StartEdgeSagaActor, StartGraphQueryActor}
import com.example.graph.saga.EdgeCreationSaga
import com.example.graph.saga.EdgeCreationSaga.{EdgeCreation, EdgeCreationReply}

object GraphActorSupervisor {
  sealed trait GraphQuerySupervisorCommand
  case class StartGraphQueryActor(graph: List[QueryReq], replyTo: ActorRef[GraphQueryReply]) extends GraphQuerySupervisorCommand
  case class StartEdgeSagaActor(nodeId: NodeId, targetNodeId: TargetNodeId, edgeType: EdgeType, properties: EdgeProperties, replyTo: ActorRef[EdgeCreationReply]) extends GraphQuerySupervisorCommand

  def apply(graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]])
    (implicit session: Session): Behavior[GraphQuerySupervisorCommand] =
    Behaviors.setup[GraphQuerySupervisorCommand](context => new GraphActorSupervisor(graphCordinator, context))

}

class GraphActorSupervisor(
  graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
  context: ActorContext[GraphQuerySupervisorCommand]
)(implicit session: Session) extends AbstractBehavior[GraphQuerySupervisorCommand](context) {
  val queryActorBehaviour =
    Behaviors
      .supervise(GraphQueryActor.GraphQueryBehaviour(graphCordinator))
      .onFailure[IllegalStateException](SupervisorStrategy.resume)

  val edgeCreationBehaviour =
    Behaviors
      .supervise(EdgeCreationSaga.EdgeCreationBehaviour(graphCordinator))
      .onFailure[IllegalStateException](SupervisorStrategy.resume)

  override def onMessage(msg: GraphQuerySupervisorCommand): Behavior[GraphQuerySupervisorCommand] = {
    msg match {
      case query: StartGraphQueryActor =>
        val queryActor = context.spawn(queryActorBehaviour, UUID.randomUUID().toString)

        queryActor ! GraphQuery(query.graph, query.replyTo)

        Behaviors.same

      case saga: StartEdgeSagaActor =>
        println("StartEdgeSagaActor")
        val sagaActor = context.spawn(edgeCreationBehaviour, UUID.randomUUID().toString)

        sagaActor ! EdgeCreation(saga.nodeId, saga.targetNodeId, saga.edgeType, saga.properties, saga.replyTo)

        Behaviors.same
    }
  }
}
