package com.example.graph.query

import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity.{EdgeDirection, EdgeProperties, EdgeType, GraphNodeCommand, GraphNodeCommandReply, NodeId, TargetNodeId}
import com.example.graph.http.Requests.QueryReq
import com.example.graph.query.GraphQueryActor.{CheckProgress, GraphQuery, GraphQueryReply}
import com.example.graph.query.GraphActorSupervisor._
import com.example.graph.query.WeightQueryActor.{WeightQuery, WeightQueryReply}
import com.example.graph.saga.EdgeCreationSaga
import com.example.graph.saga.EdgeCreationSaga.{EdgeCreation, EdgeCreationReply}

import scala.collection.mutable

object GraphActorSupervisor {
  sealed trait GraphQuerySupervisorCommand
  case class StartGraphQueryActor(graph: List[QueryReq], replyTo: ActorRef[GraphQueryReply], queryId: Option[String] = None) extends GraphQuerySupervisorCommand
  case class StartEdgeSagaActor(nodeId: NodeId, targetNodeId: TargetNodeId, edgeType: EdgeType, properties: EdgeProperties, replyTo: ActorRef[EdgeCreationReply]) extends GraphQuerySupervisorCommand
  case class GraphQueryProgress(queryId: String, replyTo: ActorRef[GraphQueryReply]) extends GraphQuerySupervisorCommand
  case class StartWeightQuery(replyTo: ActorRef[WeightQueryReply]) extends GraphQuerySupervisorCommand

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

//  val edgeCreationBehaviour =
//    Behaviors
//      .supervise(EdgeCreationSaga.EdgeCreationBehaviour(graphCordinator))
//      .onFailure[IllegalStateException](SupervisorStrategy.resume)

  val children = mutable.Map.empty[String, ActorRef[GraphQueryActor.GraphQueryCommand]]

  override def onMessage(msg: GraphQuerySupervisorCommand): Behavior[GraphQuerySupervisorCommand] = {
    msg match {
      case query: StartGraphQueryActor =>
        val queryId = query.queryId.getOrElse(UUID.randomUUID().toString)
        val queryActor = context.spawn(queryActorBehaviour, queryId)
        children.put(queryId, queryActor)
        queryActor ! GraphQuery(query.graph, query.replyTo)

        Behaviors.same

      case saga: StartEdgeSagaActor =>
//        val sagaActor = context.spawn(edgeCreationBehaviour, UUID.randomUUID().toString)

//        sagaActor ! EdgeCreation(saga.nodeId, saga.targetNodeId, saga.edgeType, saga.properties, saga.replyTo)

        Behaviors.same

      case progress: GraphQueryProgress =>
        children.get(progress.queryId).map(_ ! CheckProgress(progress.replyTo))
        Behaviors.same

      case weightQuery: StartWeightQuery =>
        val weightQueryACtor = context.spawn(WeightQueryActor.weightQueryBehaviour(graphCordinator), UUID.randomUUID().toString)
        weightQueryACtor ! WeightQuery(WeightQueryActor.planets, weightQuery.replyTo)

        Behaviors.same
    }
  }
}
