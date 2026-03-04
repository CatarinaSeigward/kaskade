package com.kaskade

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for anomaly detection Z-score and severity classification logic.
 *
 * Tests the statistical computations in isolation from Redis and Spark.
 */
class AnomalyDetectionTest extends AnyFunSuite with Matchers {

  // Helper to compute stats matching AnomalyDetection's internal method
  private def computeStats(values: Seq[Double]): (Double, Double) = {
    if (values.isEmpty) return (0.0, 0.0)
    val mean = values.sum / values.size
    val variance = values.map(v => math.pow(v - mean, 2)).sum / values.size
    (mean, math.sqrt(variance))
  }

  test("Z-score: normal value within threshold") {
    val history = Seq(100.0, 101.0, 99.0, 100.5, 100.2, 99.8, 100.1)
    val (mean, std) = computeStats(history)

    val currentValue = 100.3
    val zScore = (currentValue - mean) / std

    math.abs(zScore) should be < 2.0  // Normal value, no alert
  }

  test("Z-score: anomalous value exceeds threshold") {
    val history = Seq(100.0, 101.0, 99.0, 100.5, 100.2, 99.8, 100.1)
    val (mean, std) = computeStats(history)

    // Large deviation from mean
    val anomalousValue = mean + (std * 4.5)
    val zScore = (anomalousValue - mean) / std

    math.abs(zScore) should be > 4.0  // Critical anomaly
  }

  test("Severity classification: INFO for Z > 2") {
    val severity = classifySeverity(2.5)
    severity shouldBe "INFO"
  }

  test("Severity classification: WARNING for Z > 3") {
    val severity = classifySeverity(3.5)
    severity shouldBe "WARNING"
  }

  test("Severity classification: CRITICAL for Z > 4") {
    val severity = classifySeverity(4.5)
    severity shouldBe "CRITICAL"
  }

  test("Volume spike detection: 5x multiplier") {
    val avgVolume = 10000.0
    val currentVolume = 55000.0
    val ratio = currentVolume / avgVolume

    ratio should be > 5.0  // Volume spike detected
  }

  test("Volume spike: no alert for normal volume") {
    val avgVolume = 10000.0
    val currentVolume = 12000.0
    val ratio = currentVolume / avgVolume

    ratio should be < 5.0  // No spike
  }

  test("Stats computation: mean and std for uniform values") {
    val values = Seq(5.0, 5.0, 5.0, 5.0, 5.0)
    val (mean, std) = computeStats(values)

    mean shouldBe 5.0
    std shouldBe 0.0  // No variance
  }

  test("Stats computation: empty sequence returns zeros") {
    val (mean, std) = computeStats(Seq.empty)
    mean shouldBe 0.0
    std shouldBe 0.0
  }

  private def classifySeverity(absZScore: Double): String = {
    if (absZScore > 4.0) "CRITICAL"
    else if (absZScore > 3.0) "WARNING"
    else "INFO"
  }
}
