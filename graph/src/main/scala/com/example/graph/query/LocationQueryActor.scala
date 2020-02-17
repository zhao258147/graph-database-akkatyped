package com.example.graph.query

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import com.datastax.driver.core.Session
import com.example.graph.GraphNodeEntity.{GetLocation, GraphNodeCommand, GraphNodeCommandReply, NodeLocation}

object LocationQueryActor {
  sealed trait LocationQueryCommand
  case class LocationQuery(nodeIds: Set[String], replyTo: ActorRef[LocationQueryReply]) extends LocationQueryCommand
  case class WrappedNodeEntityResponse(nodeEntityResponse: GraphNodeCommandReply) extends LocationQueryCommand

  sealed trait LocationQueryReply
  case class Locations(collected: Map[String, NodeLocation]) extends LocationQueryReply

  val planets = Set("Sun", "Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto", "Ceres", "Eris", "Makemake", "Haumea", "Sedna", "Quaoar", "Orcus", "OR10")

  def locationQueryBehaviour(
    graphCordinator: ActorRef[ShardingEnvelope[GraphNodeCommand]],
  )(implicit session: Session): Behavior[LocationQueryCommand] = Behaviors.setup[LocationQueryCommand] { context =>
    val nodeEntityResponseMapper: ActorRef[GraphNodeCommandReply] =
      context.messageAdapter(rsp => WrappedNodeEntityResponse(rsp))

    def collectResp(collected: Map[String, NodeLocation], replyTo: ActorRef[LocationQueryReply]): Behavior[LocationQueryCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedNodeEntityResponse(rsp: NodeLocation) =>
          val cur = collected + (rsp.nodeId -> rsp)
          if(cur.keySet.size == planets.size) {
            replyTo ! Locations(cur)
            Behaviors.stopped
          } else collectResp(cur, replyTo)
      }

    val initial: Behavior[LocationQueryCommand] =
      Behaviors.receiveMessagePartial {
        case LocationQuery(nodeIds, replyTo) =>
          nodeIds.foreach{ node =>
            graphCordinator ! ShardingEnvelope(node, GetLocation(node, nodeEntityResponseMapper))
          }
          collectResp(Map.empty, replyTo)
      }

    initial
  }
}
