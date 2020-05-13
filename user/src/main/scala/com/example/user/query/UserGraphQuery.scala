package com.example.user.query

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.ShardingEnvelope
import com.example.user.UserNodeEntity.{UserCommand, UserInfo, UserReply, UserRetrievalCommand}

object UserGraphQuery {
  sealed trait UserGraphQueryCommand
  case class UserGraphQueryRequest(nodeIds: Set[String], replyTo: ActorRef[UserGraphQueryReply]) extends UserGraphQueryCommand
  case class WrappedUserEntityResponse(userEntityResponse: UserReply) extends UserGraphQueryCommand

  sealed trait UserGraphQueryReply
  case class UserNodes(collected: Set[UserInfo]) extends UserGraphQueryReply

  val names = Set(
      "u01",
      "u02",
      "u03",
      "u04",
      "u05",
      "u06",
      "u07",
      "u08",
      "u09",
      "u10",
      "u11",
      "u12",
      "u13",
      "u14",
      "u15",
      "u16",
      "u17",
      "u18",
      "u19",
      "u20",
      "u21",
      "u22",
      "u23",
      "u24",
      "u25",
      "u26",
      "u27",
      "u28",
      "u29",
      "u30",
      "u31",
      "u32",
      "u33",
      "u34",
      "u35",
      "u36",
      "u37",
      "u38",
      "u39",
      "u40",
      "u41",
      "u42",
      "u43",
      "u44",
      "u45",
      "u46",
      "u47",
      "u48",
      "u49",
      "u50",
      "u51",
      "u52",
      "u53",
      "u54",
      "u55",
      "u56",
      "u57",
      "u58",
      "u59",
      "u60",
      "u61",
      "u62",
      "u63",
      "u64",
      "u65",
      "u66",
      "u67",
      "u68",
      "u69",
      "u70",
      "u71",
      "u72",
      "u73",
      "u74",
      "u75",
      "u76",
      "u77",
      "u78",
      "u79",
      "u80",
      "u81",
      "u82",
      "u83",
      "u84",
      "u85",
      "u86",
      "u87",
      "u88",
      "u89",
      "u90",
      "u91",
      "u92",
      "u93",
      "u94",
      "u95",
      "u96",
      "u97",
      "u98"
  )

  def behaviour(
    shardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]]
  ): Behavior[UserGraphQueryCommand] = Behaviors.setup[UserGraphQueryCommand]{ context =>
    val userEntityResponseMapper: ActorRef[UserReply] =
      context.messageAdapter(rsp => WrappedUserEntityResponse(rsp))

    def collectResp(collected: Set[UserInfo], replyTo: ActorRef[UserGraphQueryReply]): Behavior[UserGraphQueryCommand] =
      Behaviors.receiveMessagePartial {
        case WrappedUserEntityResponse(rsp: UserInfo) =>
          val cur = collected + rsp
          if(cur.size == names.size) {
            replyTo ! UserNodes(cur)
            Behaviors.stopped
          } else collectResp(cur, replyTo)
      }

    val initial: Behavior[UserGraphQueryCommand] =
      Behaviors.receiveMessagePartial {
        case UserGraphQueryRequest(nodeIds, replyTo) =>
          println(nodeIds)
          nodeIds.foreach{ node =>
            println(node)
            shardRegion ! ShardingEnvelope(node, UserRetrievalCommand(node, userEntityResponseMapper))
          }
          collectResp(Set.empty, replyTo)
      }

    initial
  }
}
