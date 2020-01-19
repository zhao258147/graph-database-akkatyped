package com.example.graph.query

import java.util.UUID

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity.{GraphNodeCommand, GraphNodeCommandReply}
import com.example.graph.http.Requests.QueryReq
import com.example.graph.query.GraphQueryActor.{GraphQuery, GraphQueryReply}
import com.example.graph.query.GraphQuerySupervisor.{GraphQuerySupervisorCommand, StartGraphQueryActor}

object GraphQuerySupervisor {
  sealed trait GraphQuerySupervisorCommand
  case class StartGraphQueryActor(nodeType: String, graph: List[QueryReq], replyTo: ActorRef[GraphQueryReply]) extends GraphQuerySupervisorCommand

  def apply(graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]])
    (implicit session: Session): Behavior[GraphQuerySupervisorCommand] =
    Behaviors.setup[GraphQuerySupervisorCommand](context => new GraphQuerySupervisor(graphCordinator, context))
}

class GraphQuerySupervisor(
  graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]],
  context: ActorContext[GraphQuerySupervisorCommand]
)(implicit session: Session) extends AbstractBehavior[GraphQuerySupervisorCommand](context) {
  override def onMessage(msg: GraphQuerySupervisorCommand): Behavior[GraphQuerySupervisorCommand] = {
    msg match {
      case start: StartGraphQueryActor =>
        val queryActorBehaviour =
          Behaviors
            .supervise(GraphQueryActor.GraphQueryBehaviour(graphCordinator, start.graph))
            .onFailure[IllegalStateException](SupervisorStrategy.resume)

        val queryActor = context.spawn(queryActorBehaviour, UUID.randomUUID().toString)

        queryActor ! GraphQuery(start.replyTo)

        Behaviors.same
    }
  }
}
