package com.example.graph

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect, RetentionCriteria}
import com.example.graph.readside.ReadSideActor
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.Random

object GraphNodeEntity {
  type NodeType = String
  type TargetNodeId = String
  type NodeId = String
  type EdgeType = String
  type EdgePropertyType = String
  type EdgePropertyValue = String
  type EdgeProperties = Map[EdgePropertyType, EdgePropertyValue]
  type Edges = Map[TargetNodeId, Edge]
  type EdgesWithProperties = Map[EdgeType, Edges]
  type NodeProperties = Map[String, String]

  implicit val logger =
    LoggerFactory.getLogger(classOf[CreatedGraphNodeState])

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[To], name = "To"),
      new JsonSubTypes.Type(value = classOf[From], name = "From")
    ))
  sealed trait EdgeDirection{
    val nodeId: String
  }
  case class To(nodeId: String) extends EdgeDirection
  case class From(nodeId: String) extends EdgeDirection

  case class Edge(
    edgeType: EdgeType,
    direction: EdgeDirection,
    properties: EdgeProperties
  )

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[CreateNodeCommand], name = "CreateNodeCommand"),
      new JsonSubTypes.Type(value = classOf[UpdateNodeCommand], name = "UpdateNodeCommand"),
      new JsonSubTypes.Type(value = classOf[RemoveEdgeCommand], name = "RemoveEdgeCommand"),
      new JsonSubTypes.Type(value = classOf[UpdateEdgeCommand], name = "UpdateEdgeCommand"),
      new JsonSubTypes.Type(value = classOf[TimerCommand], name = "TimerCommand"),
      new JsonSubTypes.Type(value = classOf[GetLocation], name = "GetLocation"),
      new JsonSubTypes.Type(value = classOf[EdgeQuery], name = "EdgeQuery"),
      new JsonSubTypes.Type(value = classOf[NodeQuery], name = "NodeQuery")))
//  sealed trait GraphNodeCommand[Reply <: GraphNodeCommandReply] {
//    val nodeId: NodeId
//    def replyTo: ActorRef[Reply]
//  }
  sealed trait GraphNodeCommand {
    val nodeId: NodeId
  }
  case class CreateNodeCommand(nodeId: NodeId, nodeType: NodeType, properties: NodeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand
  case class UpdateNodeCommand(nodeId: NodeId, properties: NodeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand
  case class RemoveEdgeCommand(nodeId: NodeId, targetNodeId: TargetNodeId, edgeType: EdgeType, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand
  case class UpdateEdgeCommand(nodeId: NodeId, edgeType: EdgeType, direction: EdgeDirection, properties: EdgeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand

  case class NodeQuery(nodeId: NodeId, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand
  case class EdgeQuery(nodeId: NodeId, nodeType: Option[NodeType], nodeProperties: NodeProperties, edgeType: Option[EdgeType], edgeProperties: EdgeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand

  case class TimerCommand(nodeId: NodeId) extends GraphNodeCommand
  case class GetLocation(nodeId: NodeId, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[GraphNodeCommandSuccess], name = "GraphNodeCommandSuccess"),
      new JsonSubTypes.Type(value = classOf[GraphNodeCommandFailed], name = "GraphNodeCommandFailed"),
      new JsonSubTypes.Type(value = classOf[NodeQueryResult], name = "NodeQueryResult"),
      new JsonSubTypes.Type(value = classOf[NodeLocation], name = "NodeLocation"),
      new JsonSubTypes.Type(value = classOf[EdgeQueryResult], name = "EdgeQueryResult")))
  sealed trait GraphNodeCommandReply {
    val nodeId: NodeId
  }
  case class GraphNodeCommandSuccess(nodeId: NodeId, message: String = "") extends GraphNodeCommandReply
  case class GraphNodeCommandFailed(nodeId: NodeId, error: String) extends GraphNodeCommandReply
  case class EdgeQueryResult(nodeId: NodeId, edgeResult: Set[Edge], nodeResult: Boolean) extends GraphNodeCommandReply
  case class NodeQueryResult(nodeId: NodeId, nodeType: NodeType, properties: NodeProperties) extends GraphNodeCommandReply

  case class NodeLocation(nodeId: NodeId, x: Int, y:Int, angle: Double, mass: Double, edges: Set[Edge]) extends GraphNodeCommandReply


  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[GraphNodeEdgeRemoved], name = "GraphNodeEdgeRemoved"),
      new JsonSubTypes.Type(value = classOf[GraphNodeUpdated], name = "GraphNodeUpdated"),
      new JsonSubTypes.Type(value = classOf[MoveEvent], name = "MoveEvent"),
      new JsonSubTypes.Type(value = classOf[GraphNodeEdgeUpdated], name = "GraphNodeEdgeUpdated")))
  sealed trait GraphNodeEvent
  case class GraphNodeUpdated(id: NodeId, nodeType: NodeType, properties: NodeProperties) extends GraphNodeEvent
  case class GraphNodeEdgeRemoved(targetNodeId: TargetNodeId, edgeType: EdgeType) extends  GraphNodeEvent
  case class GraphNodeEdgeUpdated(edgeType: EdgeType, direction: EdgeDirection, properties: EdgeProperties) extends GraphNodeEvent
  case class MoveEvent(id: NodeId, x: Int, y: Int, angle: Double, cur: Int) extends GraphNodeEvent

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[EmptyGraphNodeState], name = "EmptyGraphNodeState"),
      new JsonSubTypes.Type(value = classOf[CreatedGraphNodeState], name = "CreatedGraphNodeState")))
  sealed trait GraphNodeState
  case class EmptyGraphNodeState() extends GraphNodeState
  case class CreatedGraphNodeState(
    nodeId: NodeId,
    nodeType: NodeType,
    properties: NodeProperties,
    outEdges: EdgesWithProperties,
    inEdges: EdgesWithProperties,
    x: Int,
    y: Int,
    angle: Double,
    cur: Int
  ) extends GraphNodeState

  private def commandHandler(context: ActorContext[GraphNodeCommand], timer: TimerScheduler[GraphNodeCommand]):
  (GraphNodeState, GraphNodeCommand) => ReplyEffect[GraphNodeEvent, GraphNodeState] = {
    (state, command) =>
      println("command " * 10)

      println(command)

      logger.info(s"$state")

      state match {
        case _: EmptyGraphNodeState =>
          command match {
            case create: CreateNodeCommand =>
              Effect
                .persist(GraphNodeUpdated(create.nodeId, create.nodeType, create.properties))
                .thenReply(create.replyTo)(_ => GraphNodeCommandSuccess(create.nodeId))
            case cmd =>
              Effect.noReply
          }
        case createdState: CreatedGraphNodeState =>
          command match {
            case create: CreateNodeCommand =>
              Effect.reply(create.replyTo)(GraphNodeCommandFailed(create.nodeId, "Node exists"))

            case update: UpdateNodeCommand =>
              Effect
                .persist(GraphNodeUpdated(createdState.nodeId, createdState.nodeType, update.properties))
                .thenReply(update.replyTo)(_ => GraphNodeCommandSuccess(update.nodeId))

            case removeEdge: RemoveEdgeCommand =>
              val existingEdge = for {
                targetEdges <- createdState.outEdges.get(removeEdge.edgeType)
                edges <- targetEdges.get(removeEdge.targetNodeId)
              } yield edges

              existingEdge match {
                case Some(_) =>
                  Effect
                    .persist(GraphNodeEdgeRemoved(removeEdge.targetNodeId, removeEdge.edgeType))
                    .thenReply(removeEdge.replyTo)(_ => GraphNodeCommandSuccess(removeEdge.nodeId))
                case _ =>
                  Effect.reply(removeEdge.replyTo)(GraphNodeCommandFailed(removeEdge.nodeId, "Edge does not exist"))
              }

            case updateEdge: UpdateEdgeCommand =>
              if(updateEdge.edgeType.toLowerCase == "orbit") {
                println("starting timer")
                timer.startTimerAtFixedRate(TimerCommand(updateEdge.nodeId), 500.millis)
              }
              Effect
                .persist(GraphNodeEdgeUpdated(updateEdge.edgeType, updateEdge.direction, updateEdge.properties))
                .thenReply(updateEdge.replyTo)(_ => GraphNodeCommandSuccess(updateEdge.nodeId))

            case _: TimerCommand =>
              println(createdState.nodeType)
              createdState.nodeType match {
                case "planet" | "dwarf" =>
                  val speed = 10
                  val moveEventOpt = for {
                    edges <- createdState.outEdges.get("orbit")
                    edge <- edges.values.headOption
                    op <- edge.properties.get("orbitalperiod").map(_.toDouble)
                    radius <- edge.properties.get("distance").map(_.toInt)
                  } yield {
                    val cur = createdState.cur + speed
                    val angle: Double = ((cur / op.toDouble) * 360) % 360
                    MoveEvent(createdState.nodeId, findX(angle, radius), findY(angle, radius), angle, cur)
                  }

                  moveEventOpt match {
                    case Some(m) =>
                      Effect.persist(m).thenNoReply
                    case _ =>
                      Effect.noReply
                  }
                case _ =>
                  Effect.noReply
              }

            case GetLocation(id, replyTo) =>
              val nlOpt = for {
                mass <- createdState.properties.get("mass")
                edges <- createdState.outEdges.get("orbit").map(_.values.toSet)
              } yield {
                val shortEdges = createdState.outEdges.get("shortest").map(_.values.toSet).getOrElse(Set.empty)
                NodeLocation(id, createdState.x, createdState.y, createdState.angle, mass.toDouble, shortEdges)
              }
              nlOpt match {
                case Some(nl) =>
                  Effect.reply(replyTo)(nl)
                case None =>
                  Effect.noReply
              }

            case nodeQuery: NodeQuery =>
              Effect.reply(nodeQuery.replyTo)(NodeQueryResult(createdState.nodeId, createdState.nodeType, createdState.properties))

            case checkEdge: EdgeQuery =>
              println(checkEdge)
              println(createdState)
              println(checkEdge.nodeType.forall(_ == createdState.nodeType))
              println(checkEdge.nodeProperties.toSet.subsetOf(createdState.properties.toSet))

              if(checkEdge.nodeType.forall(_ == createdState.nodeType)) {
                if(checkEdge.nodeProperties.toSet.subsetOf(createdState.properties.toSet)) {
                  val targetEdges: Edges =
                    checkEdge.edgeType.map{ edgeTypeVal =>
                      createdState.outEdges.getOrElse(edgeTypeVal, Map.empty)
                    }.getOrElse(Map.empty)

                  val edges: Set[Edge] = targetEdges.values.foldLeft(Set.empty[Edge]){
                    case (acc, edge: Edge) if checkEdge.edgeProperties.toSet.subsetOf(edge.properties.toSet) =>
                      edge.direction match {
                        case to: To =>
                          acc + edge
                        case _ => acc
                      }
                  }

                  Effect.reply(checkEdge.replyTo)(EdgeQueryResult(createdState.nodeId, edges, true))
                } else Effect.reply(checkEdge.replyTo)(EdgeQueryResult(createdState.nodeId, Set.empty, false))
              } else
                Effect.reply(checkEdge.replyTo)(EdgeQueryResult(createdState.nodeId, Set.empty, false))

          }
      }
  }

  def findX(a: Double, radius: Int): Int = {
    val radian = Math.toRadians(a)

    (radius * math.cos(radian)).intValue
  }

  def findY(a: Double, radius: Int): Int = {
    val radian = Math.toRadians(a)
    (radius * math.sin(radian)).intValue
  }

  private def eventHandler(context: ActorContext[GraphNodeCommand]): (GraphNodeState, GraphNodeEvent) => GraphNodeState = { (state, event) =>
    println("state" * 10)
    println(state)
    state match {
      case _: EmptyGraphNodeState =>
        event match {
          case created: GraphNodeUpdated =>
            CreatedGraphNodeState(
              nodeId = created.id,
              properties = created.properties,
              nodeType = created.nodeType,
              outEdges = Map.empty,
              inEdges = Map.empty,
              x = 0,
              y = 0,
              angle = 0,
              cur = 0
            )
          case _ =>
            state
        }
      case createdState: CreatedGraphNodeState =>
        event match {
          case updated: GraphNodeUpdated =>
            createdState.copy(
              nodeId = updated.id,
              properties = updated.properties,
              nodeType = updated.nodeType
            )

          case removeEdge: GraphNodeEdgeRemoved =>
            val targetEdges = createdState.outEdges.getOrElse(removeEdge.edgeType, Map.empty)
            val newEdges = targetEdges - removeEdge.targetNodeId
            if(newEdges.isEmpty)
              createdState.copy(outEdges = createdState.outEdges - removeEdge.edgeType)
            else
              createdState.copy(outEdges = createdState.outEdges + (removeEdge.edgeType -> newEdges))

          case GraphNodeEdgeUpdated(edgeType, To(nodeId), properties) =>
            val newEdge = Edge(edgeType, To(nodeId), properties)

            val newTargetEdges = createdState.outEdges.getOrElse(edgeType, Map.empty) + (nodeId -> newEdge)

            println(newTargetEdges)

            createdState.copy(
              outEdges = createdState.outEdges + (edgeType -> newTargetEdges),
            )

          case MoveEvent(_, x, y, angle, cur) =>
            println(s"($x, $y) " * 20)
            createdState.copy(
              x = x,
              y = y,
              angle = angle,
              cur = cur
            )

          case GraphNodeEdgeUpdated(edgeType, From(nodeId), properties) =>
            val rand = new Random()

            val radius = properties.getOrElse("distance", "0")
            val orbitState = if (edgeType == "orbit") {
              val angle = rand.nextInt(360)
              createdState.nodeType match {
                case "planet" | "dwarf" =>
                  createdState.copy(
                    x = findX(angle, radius.toInt),
                    y = findY(angle, radius.toInt),
                    angle = angle
                  )
                case _ =>
                  createdState
              }
            } else createdState

            println(nodeId)
            println("(x, y) " * 20)
            println(orbitState.x + " " + orbitState.y)

            val newEdge = Edge(edgeType, From(nodeId), properties)

            val newTargetEdges = createdState.inEdges.getOrElse(edgeType, Map.empty) + (nodeId -> newEdge)

            println(newTargetEdges)

            orbitState.copy(
              inEdges = createdState.outEdges + (edgeType -> newTargetEdges)
            )
        }
    }
  }

  def nodeEntityBehaviour(persistenceId: PersistenceId): Behavior[GraphNodeCommand] = Behaviors.setup { context =>
    Behaviors.withTimers { timer: TimerScheduler[GraphNodeCommand] =>
      EventSourcedBehavior(
        persistenceId,
        EmptyGraphNodeState(),
        commandHandler(context, timer),
        eventHandler(context)
      )
        .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 20, keepNSnapshots = 2))
        .withTagger{
//          case _: GraphNodeUpdated => Set(ReadSideActor.NodeUpdateEventName)
          case _: MoveEvent => Set(ReadSideActor.MoveEventName)
          case _ => Set.empty
        }
    }

  }
}
