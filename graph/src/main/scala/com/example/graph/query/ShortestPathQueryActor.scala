package com.example.graph.query

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.util.Timeout
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity.{GraphNodeCommand, GraphNodeCommandReply, TargetNodeId}
import scala.concurrent.duration._

object ShortestPathQueryActor {
  sealed trait ShortestPathCommand
  case class ShortestDistanceQuery(replyTo: ActorRef[ShortestPathReply]) extends ShortestPathCommand
  case class WrappedNodeEntityResponse(nodeEntityResponse: GraphNodeCommandReply) extends ShortestPathCommand

  sealed trait ShortestPathReply
  case class GraphQueryReplySuccess(list: Set[Seq[TargetNodeId]]) extends ShortestPathReply
  case class GraphQueryReplyFailed(error: String) extends ShortestPathReply

  def ShortestPathBehaviour(
    graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand]],
    start: TargetNodeId,
    end: TargetNodeId
  )(implicit session: Session): Behavior[ShortestPathCommand] = Behaviors.setup[ShortestPathCommand] { context =>
    implicit val system = context.system
    implicit val timeout: Timeout = 30.seconds
    implicit val ex = context.executionContext



    Behaviors.stopped
  }

}
