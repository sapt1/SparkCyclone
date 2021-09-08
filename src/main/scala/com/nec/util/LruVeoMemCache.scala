package com.nec.util
import java.util.LinkedHashMap
import scala.collection.mutable
import com.nec.aurora.Aurora

class LruVeoMemCache(val maxSize: Int) {
  private val map: LinkedHashMap[(Aurora.veo_proc_handle, Long), Long] =
    new LinkedHashMap[(Aurora.veo_proc_handle, Long), Long](maxSize, 0.75f, true) {
      override def removeEldestEntry(
        e: java.util.Map.Entry[(Aurora.veo_proc_handle, Long), Long]
      ): Boolean = {
        if (size > maxSize) {
          val (proc, name) = e.getKey()
          val ptr = e.getValue()

          Aurora.veo_free_mem(proc, ptr)

          map.remove(e.getKey())
        }
        false
      }
    }

  def apply(veo_proc_handle: Aurora.veo_proc_handle, key: Long): Option[Long] = {
    None
    /*if (map.containsKey((veo_proc_handle, key))) {
            Some(map.get((veo_proc_handle, key)))
        } else {
            None
        }*/
  }

  def get(veo_proc_handle: Aurora.veo_proc_handle, key: Long): Long = {
    map.get((veo_proc_handle, key))
  }

  def put(veo_proc_handle: Aurora.veo_proc_handle, key: Long, value: Long): Long = {
    map.put((veo_proc_handle, key), value)
  }

  def contains(veo_proc_handle: Aurora.veo_proc_handle, key: Long): Boolean = {
    map.containsKey((veo_proc_handle, key))
  }
}
