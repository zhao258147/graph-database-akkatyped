package com.example.graph

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect, RetentionCriteria}
import com.example.graph.readside.{ClickReadSideActor, EventTags, NodeReadSideActor}
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.lightbend.cinnamon.akka.typed.CinnamonMetrics
import com.lightbend.cinnamon.metric.{Counter, Rate, Recorder}

object GraphNodeEntity {
  //TODO: change all primitive types to value classes, so we have types for strings
  type NodeType = String
  type Tag = String
  type Weight = Int
  type TargetNodeId = String
  type NodeId = String
  type EdgeType = String
  type EdgePropertyType = String
  type EdgePropertyValue = String
  type Tags = Map[Tag, Int]
  type EdgeProperties = Map[EdgePropertyType, EdgePropertyValue]
  type Edges = Map[TargetNodeId, Edge]
  type EdgesWithProperties = Map[EdgeType, Edges]
  type NodeProperties = Map[String, String]

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

  val ReferralEdgeType = "referral"

  case class Edge(edgeType: EdgeType, direction: EdgeDirection, properties: EdgeProperties, labels: Map[Tag, Weight], weight: Int, visitors: Set[String])

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[CreateNodeCommand], name = "CreateNodeCommand"),
      new JsonSubTypes.Type(value = classOf[UpdateNodeCommand], name = "UpdateNodeCommand"),
      new JsonSubTypes.Type(value = classOf[RemoveEdgeCommand], name = "RemoveEdge"),
      new JsonSubTypes.Type(value = classOf[EdgeQuery], name = "EdgeQuery"),
      new JsonSubTypes.Type(value = classOf[UpdateEdgeCommand], name = "UpdateEdge")))
  sealed trait GraphNodeCommand[Reply <: GraphNodeCommandReply] {
    val nodeId: NodeId
    val replyTo: ActorRef[Reply]
  }
  case class CreateNodeCommand(nodeId: String, nodeType: NodeType, companyId: String, tags: Tags, properties: NodeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class UpdateNodeCommand(nodeId: String, tags: Tags, properties: NodeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class RemoveEdgeCommand(nodeId: String, targetNodeId: TargetNodeId, edgeType: EdgeType, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class UpdateEdgeCommand(nodeId: String, edgeType: EdgeType, direction: EdgeDirection, properties: EdgeProperties, userId: String, visitorLabels: Option[Map[Tag, Weight]], weight: Option[Weight], replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]

  case class NodeQuery(nodeId: String, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class EdgeQuery(nodeId: String, nodeType: Option[NodeType] = None, nodeProperties: NodeProperties, edgeTypes: Set[EdgeType], edgeProperties: EdgeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class QueryRecommended(nodeId: String, referrerNodeId: Option[String] = None, visitorId: String, edgeTypes: Set[EdgeType], visitorLabels: Map[Tag, Weight], edgeProperties: EdgeProperties = Map.empty, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[GraphNodeCommandSuccess], name = "GraphNodeCommandSuccess"),
      new JsonSubTypes.Type(value = classOf[GraphNodeCommandFailed], name = "GraphNodeCommandFailed"),
      new JsonSubTypes.Type(value = classOf[EdgeQueryResult], name = "EdgeQueryResult")))
  sealed trait GraphNodeCommandReply {
    val nodeId: NodeId
  }
  case class GraphNodeCommandSuccess(nodeId: NodeId, message: String = "") extends GraphNodeCommandReply
  case class GraphNodeCommandFailed(nodeId: NodeId, error: String) extends GraphNodeCommandReply
  case class EdgeQueryResult(nodeId: NodeId, nodeType: NodeType, tags: Tags, edgeResult: Set[Edge], nodeResult: Boolean) extends GraphNodeCommandReply
  case class RecommendedResult(nodeId: NodeId, nodeType: NodeType, tags: Tags, similarUserEdges: Seq[Edge], popularEdges: Seq[Edge], clicks: Int = 0, uniqueVisitors: Int = 0) extends GraphNodeCommandReply
  case class NodeQueryResult(nodeId: NodeId, nodeType: NodeType, properties: NodeProperties, tags: Tags, edges: Set[Edge], visitors: Set[String]) extends GraphNodeCommandReply

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[GraphNodeEdgeRemoved], name = "GraphNodeEdgeRemoved"),
      new JsonSubTypes.Type(value = classOf[GraphNodeUpdated], name = "GraphNodeUpdated"),
      new JsonSubTypes.Type(value = classOf[GraphNodeClickUpdated], name = "GraphNodeClickUpdated"),
      new JsonSubTypes.Type(value = classOf[GraphNodeVisitorUpdated], name = "GraphNodeVisitorUpdated"),
      new JsonSubTypes.Type(value = classOf[GraphNodeEdgeUpdated], name = "GraphNodeEdgeUpdated")))
  sealed trait GraphNodeEvent
  case class GraphNodeUpdated(id: NodeId, nodeType: NodeType, companyId: String, tags: Tags, properties: NodeProperties) extends GraphNodeEvent
  case class GraphNodeEdgeRemoved(targetNodeId: TargetNodeId, edgeType: EdgeType) extends  GraphNodeEvent
  case class GraphNodeEdgeUpdated(edgeType: EdgeType, direction: EdgeDirection, properties: EdgeProperties, visitorId: String, visitorLabels: Map[Tag, Weight], weight: Option[Int] = None) extends GraphNodeEvent
  case class GraphNodeClickUpdated(nodeId: NodeId, companyId: String, nodeType: NodeType, tags: Tags, ts: Long, clicks: Int) extends GraphNodeEvent
  case class GraphNodeVisitorUpdated(visitorId: String, ts: Long) extends GraphNodeEvent

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
    companyId: String,
    properties: NodeProperties,
    outEdges: EdgesWithProperties,
    inEdges: EdgesWithProperties,
    tags: Tags,
    uniqueVisitors: Set[String] = Set.empty,
    activeVisitors: Map[String, Long] = Map.empty,
    clicks: Int = 0,
    previousClicks: Int = 0,
    previousClickCommit: Long = System.currentTimeMillis()
  ) extends GraphNodeState

  private def commandHandler(context: ActorContext[GraphNodeCommand[GraphNodeCommandReply]]):
  (GraphNodeState, GraphNodeCommand[GraphNodeCommandReply]) => ReplyEffect[GraphNodeEvent, GraphNodeState] = {
    (state, command) =>
      context.log.debug(s"$command")
      context.log.debug(s"$state")

      state match {
        case _: EmptyGraphNodeState =>
          command match {
            case create: CreateNodeCommand =>
              Effect
                .persist(GraphNodeUpdated(create.nodeId, create.nodeType, create.companyId, create.tags, create.properties))
                .thenReply(create.replyTo)(_ => GraphNodeCommandSuccess(create.nodeId))
            case cmd =>
              Effect.reply(cmd.replyTo)(GraphNodeCommandFailed(cmd.nodeId, "Node does not exist"))
          }
        case createdState: CreatedGraphNodeState =>
          command match {
            case create: CreateNodeCommand =>
              Effect.reply(create.replyTo)(GraphNodeCommandFailed(create.nodeId, "Node exists"))

            case update: UpdateNodeCommand =>
              Effect
                .persist(GraphNodeUpdated(createdState.nodeId, createdState.nodeType, createdState.companyId, update.tags, update.properties))
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
              val visitorLabels = updateEdge.visitorLabels.getOrElse(createdState.tags.keys.map(_ -> 1)).toMap
              val evt = GraphNodeEdgeUpdated(updateEdge.edgeType, updateEdge.direction, updateEdge.properties, updateEdge.userId, visitorLabels, updateEdge.weight)
              val ts = System.currentTimeMillis()
              val incrementalClicks = createdState.clicks - createdState.previousClicks
              if(ts - createdState.previousClickCommit > 60000 || incrementalClicks > 10)
                Effect
                  .persist(
                    evt,
                    GraphNodeClickUpdated(createdState.nodeId, createdState.companyId, createdState.nodeType, createdState.tags, ts, incrementalClicks)
                  )
                  .thenReply(updateEdge.replyTo)(_ => GraphNodeCommandSuccess(updateEdge.nodeId))
              else Effect.persist(evt).thenReply(updateEdge.replyTo)(_ => GraphNodeCommandSuccess(updateEdge.nodeId))


            case nodeQuery: NodeQuery =>
              Effect.reply(nodeQuery.replyTo)(NodeQueryResult(createdState.nodeId, createdState.nodeType, createdState.properties, createdState.tags, createdState.outEdges.values.toList.flatMap(_.values).toSet, createdState.uniqueVisitors))

            case query: QueryRecommended =>
              val targetEdges: Iterable[Edge] = createdState.outEdges.values.flatMap(_.values)
              val edges = targetEdges.filter(x => query.edgeProperties.toSet.subsetOf(x.properties.toSet)).toSeq

              val sortedEdges: Seq[Edge] = edges.sortWith(_.weight > _.weight)

              val visitorTotalWeight = query.visitorLabels.values.sum + 1

              val recommended: Seq[Edge] = edges.foldLeft(Seq.empty[(Edge, Weight)]){
                case (acc, curEdge) =>
                  val weightTotal = query.visitorLabels.foldLeft(0){
                    case (weightAcc, (label, weight)) =>
                      weightAcc + (curEdge.labels.getOrElse(label, 0) * (weight.toDouble / visitorTotalWeight)).toInt
                  }
                  (curEdge -> weightTotal) +: acc
              }.sortWith(_._2 > _._2).map(_._1)

              val edgeQueryResult = RecommendedResult(createdState.nodeId, createdState.nodeType, createdState.tags, recommended.take(20), sortedEdges.take(20), createdState.clicks, createdState.uniqueVisitors.size)
              val visitorUpdate = GraphNodeVisitorUpdated(query.visitorId, System.currentTimeMillis())
              query.referrerNodeId match {
                case Some(referrer) =>
                  Effect.persist(
                    visitorUpdate,
                    GraphNodeEdgeUpdated(ReferralEdgeType, From(referrer), Map.empty, query.visitorId, query.visitorLabels)
                  ).thenReply(query.replyTo)(_ => edgeQueryResult)

                case None =>
                  Effect.persist(visitorUpdate).thenReply(query.replyTo)(_ => edgeQueryResult)
              }

            case checkEdge: EdgeQuery =>
              if(checkEdge.nodeType.forall(_ == createdState.nodeType)) {
                if(checkEdge.nodeProperties.toSet.subsetOf(createdState.properties.toSet)) {
                  val targetEdges: Edges = createdState.outEdges.filter{
                    case (edgeType, _) =>
                      checkEdge.edgeTypes.contains(edgeType) || checkEdge.edgeTypes.isEmpty
                  }.values.flatten.toMap

                  val edges: Set[Edge] = targetEdges.values.foldLeft(Set.empty[Edge]){
                    case (acc, edge: Edge) if checkEdge.edgeProperties.toSet.subsetOf(edge.properties.toSet) =>
                      edge.direction match {
                        case to: To =>
                          acc + edge
                        case _ => acc
                      }
                  }

                  val edgeQueryResult = EdgeQueryResult(createdState.nodeId, createdState.nodeType, createdState.tags, edges, true)

                  Effect.reply(checkEdge.replyTo)(edgeQueryResult)
                } else Effect.reply(checkEdge.replyTo)(EdgeQueryResult(createdState.nodeId, createdState.nodeType, createdState.tags, Set.empty, false))
              } else
                Effect.reply(checkEdge.replyTo)(EdgeQueryResult(createdState.nodeId, createdState.nodeType, createdState.tags, Set.empty, false))

          }
      }
  }

  private def eventHandler(context: ActorContext[GraphNodeCommand[GraphNodeCommandReply]]): (GraphNodeState, GraphNodeEvent) => GraphNodeState = { (state, event) =>
    state match {
      case _: EmptyGraphNodeState =>
        event match {
          case created: GraphNodeUpdated =>
            CreatedGraphNodeState(
              nodeId = created.id,
              properties = created.properties,
              nodeType = created.nodeType,
              companyId = created.companyId,
              outEdges = Map.empty,
              inEdges = Map.empty,
              tags = created.tags
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

          case GraphNodeEdgeUpdated(edgeType, To(nodeId), properties, visitorId, labels, weight) =>
            val edgeOption: Option[Edge] = createdState.outEdges.get(edgeType).flatMap(_.get(nodeId))
            val updatedEdge = edgeOption.map{ edge =>
              val updatedLabels =
                if(edge.visitors.contains(visitorId))
                  edge.labels
                else
                  edge.labels.foldLeft(Map.empty[Tag, Weight]){
                    case (acc, (tag, weight)) =>
                      acc + (tag -> (weight * edge.visitors.size + labels.getOrElse(tag, 0)))
                  }

              edge.copy(
                labels = updatedLabels,
                weight = edge.weight + weight.getOrElse(1),
                visitors = edge.visitors + visitorId
              )
            }.getOrElse(Edge(edgeType, To(nodeId), properties, labels, 1, Set(visitorId)))

            val targetEdges = createdState.outEdges.getOrElse(edgeType, Map.empty)
            val newTargetEdges = targetEdges + (nodeId -> updatedEdge)

            context.log.debug(newTargetEdges.toString)

            val newUniqueVisitors = createdState.uniqueVisitors + visitorId

            val clickrate: Rate = CinnamonMetrics(context).createRate("clickrate", Map("nodeId" -> createdState.nodeId, "nodeType" -> createdState.nodeId, "companyId" -> createdState.companyId))
            val uniquevisitorCounter: Recorder = CinnamonMetrics(context).createRecorder("uniquevisitors", Map("nodeId" -> createdState.nodeId, "nodeType" -> createdState.nodeId, "companyId" -> createdState.companyId))
            val edgesCounter: Counter = CinnamonMetrics(context).createCounter("edges", Map("nodeId" -> createdState.nodeId, "nodeType" -> createdState.nodeId, "companyId" -> createdState.companyId))
            val visitLengthRecorder: Recorder = CinnamonMetrics(context).createRecorder("visitlength", Map("nodeId" -> createdState.nodeId, "nodeType" -> createdState.nodeId, "companyId" -> createdState.companyId))

            clickrate.mark()
            uniquevisitorCounter.record(newUniqueVisitors.size)
            if(!targetEdges.contains(nodeId)) edgesCounter.increment()
            createdState.activeVisitors.get(visitorId) foreach { ts =>
              visitLengthRecorder.record(System.currentTimeMillis() - ts)
            }

            createdState.copy(
              outEdges = createdState.outEdges + (edgeType -> newTargetEdges),
              uniqueVisitors = newUniqueVisitors,
              clicks = createdState.clicks + 1,
              activeVisitors = createdState.activeVisitors - visitorId
            )

          case GraphNodeEdgeUpdated(edgeType, From(nodeId), properties, visitorId, labels, weight) =>
            val edgeOption: Option[Edge] = createdState.inEdges.get(edgeType).flatMap(_.get(nodeId))
            val updatedEdge = edgeOption.map{ edge =>
              val updatedLabels =
                if(edge.visitors.contains(visitorId))
                  edge.labels
                else
                  edge.labels.foldLeft(Map.empty[Tag, Weight]){
                    case (acc, (tag, weight)) =>
                      acc + (tag -> (weight + labels.getOrElse(tag, 0)))
                  }

              edge.copy(
                labels = updatedLabels,
                weight = edge.weight + weight.getOrElse(1),
                visitors = edge.visitors + visitorId
              )
            }.getOrElse(Edge(edgeType, From(nodeId), properties, labels, 1, Set(visitorId)))

            val targetEdges = createdState.inEdges.getOrElse(edgeType, Map.empty)
            val newTargetEdges = targetEdges + (nodeId -> updatedEdge)

            context.log.debug(newTargetEdges.toString)

            createdState.copy(
              inEdges = createdState.inEdges + (edgeType -> newTargetEdges)
            )

          case clickUpdate: GraphNodeClickUpdated =>
            createdState.copy(
              previousClicks = createdState.clicks,
              previousClickCommit = clickUpdate.ts
            )

          case GraphNodeVisitorUpdated(visitorId, ts) =>
            createdState.copy(
              activeVisitors = createdState.activeVisitors + (visitorId -> ts)
            )
        }
    }
  }

  val TypeKey = EntityTypeKey[GraphNodeCommand[GraphNodeCommandReply]]("graph")

  def nodeEntityBehaviour(persistenceId: PersistenceId): Behavior[GraphNodeCommand[GraphNodeCommandReply]] = Behaviors.setup { context =>
    EventSourcedBehavior.withEnforcedReplies(
      persistenceId,
      EmptyGraphNodeState(),
      commandHandler(context),
      eventHandler(context)
    )
      .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 20, keepNSnapshots = 2))
      .withTagger{
        case _: GraphNodeUpdated => Set(NodeReadSideActor.NodeUpdateEventName, EventTags.CommonEvtTag)
        case _: GraphNodeClickUpdated => Set(ClickReadSideActor.ClickUpdateEventName, EventTags.CommonEvtTag)
        case _ => Set(EventTags.CommonEvtTag)
      }
  }
}
