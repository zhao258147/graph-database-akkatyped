package com.example.user

import java.util.UUID

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.persistence.typed.PersistenceId
import akka.stream.ActorMaterializer
import com.datastax.driver.core.{Cluster, Session}
import com.example.user.UserNodeEntity.{UserCommand, UserReply}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import com.example.user.config.UserConfig
import com.example.user.http.{CORSHandler, RequestApi}
import com.example.user.query.UserGraphQuery
import com.example.user.query.UserGraphQuery.{UserGraphQueryReply, UserGraphQueryRequest}
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContextExecutor

object Main extends App {
  import akka.actor.typed.scaladsl.adapter._

  val conf = ConfigFactory.load()

  implicit val typedSystem: ActorSystem[UserMainCommand] = ActorSystem(Main(), "RayDemo")

  implicit val system = typedSystem.toClassic
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  AkkaManagement(system).start()
  ClusterBootstrap(system).start()

  val config = conf.as[UserConfig]("UserConfig")
  implicit val session: Session = Cluster.builder
    .addContactPoint(config.cassandraConfig.contactPoints)
    .withPort(config.cassandraConfig.port)
    .withCredentials(config.cassandraConfig.username, config.cassandraConfig.password)
    .build
    .connect()

  val sharding = ClusterSharding(typedSystem)

  val settings = ClusterShardingSettings(typedSystem)

  val shardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]] =
    sharding.init(Entity(UserNodeEntity.TypeKey)(
      createBehavior = entityContext =>
        UserNodeEntity.userEntityBehaviour(
          PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)
        )
    ).withRole("user"))

  sealed trait UserMainCommand
  case class UserGraphRequest(users: Set[String], replayTo: ActorRef[UserGraphQueryReply]) extends UserMainCommand

  def apply(): Behavior[UserMainCommand] = Behaviors.setup{ context =>
    Behaviors.receiveMessage{
      case UserGraphRequest(users, replyTo) =>
        val queryActor = context.spawn(UserGraphQuery.behaviour(shardRegion), UUID.randomUUID().toString)
        queryActor ! UserGraphQueryRequest(users, replyTo)
        Behaviors.same
    }
  }

  val route: Route = RequestApi.route(shardRegion)

  private val cors = new CORSHandler {}

  Http().bindAndHandle(
    cors.corsHandler(route),
    config.http.interface,
    config.http.port
  )
}
