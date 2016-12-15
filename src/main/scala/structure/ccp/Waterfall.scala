package structure.ccp

import structure.ccp.Waterfall.WaterfallStage

/**
  * Created by dennis on 15/12/16.
  */
case class Waterfall(stages: Map[WaterfallStage, WaterfallStage]) {
  def next(s: WaterfallStage): Option[WaterfallStage] = stages.get(s)
}

object Waterfall {
  trait WaterfallStage
  case object Start extends WaterfallStage
  case object Defaulted extends WaterfallStage
  case object Survivors extends WaterfallStage
  case object Unfunded extends WaterfallStage
  case object FirstLevelEquity extends WaterfallStage
  case object SecondLevelEquity extends WaterfallStage
  case object VMGH extends WaterfallStage
  case object End extends WaterfallStage
}
