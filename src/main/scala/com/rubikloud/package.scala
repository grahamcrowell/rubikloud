package com

package object rubikloud {
  // centralize file system prefix for path
  private val azure_prefix = "wasb:///"
  private val local_prefix = ""
  val path_prefix = local_prefix
}
