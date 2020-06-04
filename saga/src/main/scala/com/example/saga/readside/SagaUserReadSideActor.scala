package com.example.saga.readside

import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query._
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core._
import com.example.user.UserNodeEntity
import com.example.user.UserNodeEntity.UserUpdated
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}

import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContextExecutor

object SagaUserReadSideActor {
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[RetrieveUsersQuery], name = "RetrieveUsersQuery"),
      new JsonSubTypes.Type(value = classOf[UserInformationUpdate], name = "UserInformationUpdate")))
  sealed trait SagaUserReadSideCommand
  case class UserInformationUpdate(userInfo: UserUpdated) extends SagaUserReadSideCommand
  case class RetrieveUsersQuery(replyTo: ActorRef[SagaUserInfoResponse], userIds: Set[String]) extends SagaUserReadSideCommand

  case class SagaUserInfoResponse(list: Set[UserUpdated])

  def apply()(implicit session: Session): Behavior[SagaUserReadSideCommand] =
    Behaviors.setup[SagaUserReadSideCommand] { context =>
      implicit val system: ActorSystem[Nothing] = context.system
      implicit val ec: ExecutionContextExecutor = system.executionContext

//      println("SagaUserReadSideActor started")

      val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      val createdStream: Source[EventEnvelope, NotUsed] = queries.eventsByTag(UserNodeEntity.UserUpdateTagName, NoOffset)
      createdStream
        .map {
          case ee@EventEnvelope(_, _, _, value: UserUpdated) =>
//            println(value)
            context.self ! UserInformationUpdate(value)
            ee
          case ee =>
            ee
        }
        .runWith(Sink.ignore)

      def updatesAndQueries(userMap: HashMap[String, UserUpdated]): Behavior[SagaUserReadSideCommand] =
        Behaviors.receiveMessagePartial{
          case UserInformationUpdate(node) =>
//            println(node)
            updatesAndQueries(userMap + (node.userId -> node))

          case RetrieveUsersQuery(replyTo, userIds) =>
            val userInfoSet: Set[UserUpdated] = userIds.flatMap( uid =>
              userMap.get(uid)
            )
            replyTo ! SagaUserInfoResponse(userInfoSet)
            Behaviors.same
        }

      updatesAndQueries(HashMap.empty)
    }

}
