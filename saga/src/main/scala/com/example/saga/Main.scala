package com.example.saga

import java.util.UUID

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, SupervisorStrategy}
import akka.cluster.sharding.typed.internal.protobuf.ShardingMessages
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import akka.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.actor.typed.scaladsl.AskPattern._

import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.persistence.typed.PersistenceId
import akka.stream.ActorMaterializer
import com.datastax.driver.core.{Cluster, Session}
import com.example.graph.GraphNodeEntity
import com.example.graph.GraphNodeEntity.{GraphNodeCommand, GraphNodeCommandReply, NodeId}
import com.example.graph.readside.{ClickReadSideActor, NodeReadSideActor, OffsetManagement}
import com.example.saga.HomePageRecommendationActor.{HomePageRecommendation, HomePageRecommendationReply}
import com.example.saga.NodeBookmarkActor.{BookmarkNode, NodeBookmarkReply}
import com.example.saga.NodeRecommendationActor.{NodeRecommendationReply, NodeReferral}
import com.example.saga.UserBookmarkActor.{BookmarkUser, UserBookmarkReply}
import com.example.saga.config.SagaConfig
import com.example.saga.http.{CORSHandler, RequestApi}
import com.example.saga.readside.{SagaNodeReadSideActor, SagaTrendingNodesActor, SagaUserReadSideActor}
import com.example.user.UserNodeEntity
import com.example.user.UserNodeEntity._
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

import scala.concurrent.ExecutionContextExecutor

object Main extends App {
  val conf = ConfigFactory.load()
  val config = conf.as[SagaConfig]("SagaConfig")

  implicit val userParams = config.userEntityParams
  implicit val nodeParams = config.nodeEntityParams

  import akka.actor.typed.scaladsl.adapter._

  implicit val typedSystem: ActorSystem[SagaCommand] = ActorSystem(Main(), "RayDemo")
  implicit val classSystem = typedSystem.toClassic
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  implicit val ec: ExecutionContextExecutor = typedSystem.executionContext

  AkkaManagement(classSystem).start()
  ClusterBootstrap(classSystem).start()

  implicit val session: Session = Cluster.builder
    .addContactPoint(config.cassandraConfig.contactPoints)
    .withPort(config.cassandraConfig.port)
    .withCredentials(config.cassandraConfig.username, config.cassandraConfig.password)
    .build
    .connect()

  val sharding = ClusterSharding(typedSystem)

  val settings = ClusterShardingSettings(typedSystem)

  val graphShardRegion: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]] =
    sharding.init(Entity(GraphNodeEntity.TypeKey)(
      createBehavior = entityContext =>
        GraphNodeEntity.nodeEntityBehaviour(
          PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)
        )
    ).withRole("graph"))

  val userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]] =
    sharding.init(Entity(UserNodeEntity.TypeKey)(
      createBehavior = entityContext =>
        UserNodeEntity.userEntityBehaviour(
          PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)
        )
    ).withRole("user"))

  val offsetManagement = new OffsetManagement

  val singletonManager = ClusterSingleton(typedSystem)
  val nodeReadSideActor = singletonManager.init(
    SingletonActor(
      Behaviors.supervise(
        NodeReadSideActor.ReadSideActorBehaviour(
          config.readSideConfig,
          offsetManagement
        )
      ).onFailure[Exception](SupervisorStrategy.restart), "NodeReadSide"))

  val clickReadSideActor: ActorRef[ClickReadSideActor.ClickStatCommands] = singletonManager.init(
    SingletonActor(
      Behaviors.supervise(
        ClickReadSideActor.behaviour(
          config.readSideConfig,
          offsetManagement
        )
      ).onFailure[Exception](SupervisorStrategy.restart), "ClickReadSide"))



  sealed trait SagaCommand
  case class NodeRecoCommand(nodeId: NodeId, userId: UserId, userLabels: Map[String, Int], bias: NodeVisitBias, replyTo: ActorRef[NodeRecommendationReply]) extends SagaCommand
  case class HomePageRecoCommand(userId: UserId, userLabels: Map[String, Int], replyTo: ActorRef[HomePageRecommendationReply]) extends SagaCommand
  case class BookmarkUserCommand(userId: UserId, targetUserId: UserId, replyTo: ActorRef[UserBookmarkReply]) extends SagaCommand
  case class BookmarkNodeCommand(nodeId: NodeId, userId: UserId, replyTo: ActorRef[NodeBookmarkReply]) extends SagaCommand

//  case class RemoveBookmarkUserCommand(userId: UserId, targetNodeId: UserId, replyTo: ActorRef[UserBookmarkReply]) extends SagaCommand
//  case class RemoveBookmarkNodeCommand(nodeId: NodeId, userId: UserId, replyTo: ActorRef[NodeBookmarkReply]) extends SagaCommand


  def apply(): Behavior[SagaCommand] = Behaviors.setup{ context =>
    val sagaNodeReadSideActor: ActorRef[SagaNodeReadSideActor.SagaNodeReadSideCommand] = context.spawn(SagaNodeReadSideActor(nodeParams.numberOfRecommendationsToTake), "LocalNodeReadSideActor")

    Behaviors.receiveMessage{
      case referral: NodeRecoCommand =>
        val sagaActor = context.spawn(NodeRecommendationActor(graphShardRegion, userShardRegion, clickReadSideActor, sagaNodeReadSideActor), UUID.randomUUID().toString)
        sagaActor ! NodeReferral(referral.nodeId, referral.userId, referral.userLabels, referral.bias, referral.replyTo)
        Behaviors.same

      case home: HomePageRecoCommand =>
        val sagaActor = context.spawn(HomePageRecommendationActor(userShardRegion, clickReadSideActor, sagaNodeReadSideActor), UUID.randomUUID().toString)
        sagaActor ! HomePageRecommendation(home.userId, home.userLabels, home.replyTo)
        Behaviors.same

      case bookmarkUser: BookmarkUserCommand =>
        val sagaActor = context.spawn(UserBookmarkActor(userShardRegion), UUID.randomUUID().toString)
        sagaActor ! BookmarkUser(bookmarkUser.userId, bookmarkUser.targetUserId, bookmarkUser.replyTo)
        Behaviors.same

      case bookmarkNode: BookmarkNodeCommand =>
        val sagaActor = context.spawn(NodeBookmarkActor(graphShardRegion, userShardRegion), UUID.randomUUID().toString)
        sagaActor ! BookmarkNode(bookmarkNode.nodeId, bookmarkNode.userId, bookmarkNode.replyTo)
        Behaviors.same

    }
  }

  val route: Route = RequestApi.route(graphShardRegion, userShardRegion)

  private val cors = new CORSHandler {}

  Http().bindAndHandle(
    cors.corsHandler(route),
    config.http.interface,
    config.http.port
  )

}
