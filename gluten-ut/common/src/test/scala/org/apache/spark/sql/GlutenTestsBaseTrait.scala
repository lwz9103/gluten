/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql

import io.glutenproject.test.TestStats
import io.glutenproject.utils.BackendTestSettings

import org.apache.spark.SparkFunSuite

import org.scalactic.source.Position
import org.scalatest.{Args, Status, Tag}

trait GlutenTestsBaseTrait extends SparkFunSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val basePath: String = rootPath + "unit-tests-working-home"

  protected val warehouse: String = basePath + "/spark-warehouse"
  protected val metaStorePathAbsolute: String = basePath + "/meta"

  // The blacklist is taken in highest priority. Tests on the
  // list will never be run with no regard to backend test settings.
  def testNameBlackList: Seq[String] = Seq()

  def disableFallback: Boolean = true

  def withFallbackCheck(testFun: => Any): Any = {
    testFun
    if (disableFallback && !TestStats.offloadGluten) {
      assert(false, "Fallback Disabled")
    }
  }

  def shouldRun(testName: String): Boolean = {
    if (testNameBlackList.exists(_.equalsIgnoreCase(GlutenTestConstants.IGNORE_ALL))) {
      return false
    }
    if (testNameBlackList.contains(testName)) {
      return false
    }
    BackendTestSettings.shouldRun(getClass.getCanonicalName, testName)
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (shouldRun(testName)) {
      super.test(testName, testTags: _*)(withFallbackCheck(testFun))
    }
  }

  override protected def runTest(testName: String, args: Args): Status = {
    TestStats.suiteTestNumber += 1
    TestStats.offloadGluten = true
    TestStats.startCase(testName)
    val status = super.runTest(testName, args)
    if (TestStats.offloadGluten) {
      TestStats.offloadGlutenTestNumber += 1
      print("'" + testName + "'" + " offload to gluten\n")
    } else {
      // you can find the keyword 'Validation failed for' in function doValidate() in log
      // to get the fallback reason
      print("'" + testName + "'" + " NOT use gluten\n")
      TestStats.addFallBackCase()
    }

    TestStats.endCase(status.succeeds());
    status
  }
}
