package com.example.user

import akka.actor.ActorSystem
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.persistence.typed.PersistenceId
import akka.stream.ActorMaterializer
import com.datastax.driver.core.{Cluster, Session}
import com.example.user.UserNodeEntity.{UserCommand, UserReply}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import com.example.user.config.UserConfig
import com.example.user.http.{CORSHandler, RequestApi}
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContextExecutor

object Main extends App {
  import akka.actor.typed.scaladsl.adapter._

  val conf = ConfigFactory.load()

  implicit val system: ActorSystem = ActorSystem("RayDemo", conf)
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  implicit val typedSystem: akka.actor.typed.ActorSystem[Nothing] = system.toTyped

  val config = conf.as[UserConfig]("UserConfig")
  implicit val session: Session = Cluster.builder
    .addContactPoint(config.cassandraConfig.contactPoints)
    .withPort(config.cassandraConfig.port)
    .build
    .connect()

  val sharding = ClusterSharding(typedSystem)

  val settings = ClusterShardingSettings(typedSystem)

  val TypeKey = EntityTypeKey[UserCommand[UserReply]]("user")

  val shardRegion =
    sharding.init(Entity(TypeKey)(
      createBehavior = entityContext =>
        UserNodeEntity.userEntityBehaviour(
          PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)
        )
    ))

  val route: Route = RequestApi.route(shardRegion)

  private val cors = new CORSHandler {}

  Http()(system).bindAndHandle(
    cors.corsHandler(route),
    config.http.interface,
    config.http.port
  )

}
