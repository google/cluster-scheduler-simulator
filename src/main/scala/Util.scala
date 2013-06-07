package ClusterSchedulingSimulation

object Seed {
  private var seed: Long = 0
  def set(newSeed: Long) = {seed = newSeed}
  def apply() = seed
}
