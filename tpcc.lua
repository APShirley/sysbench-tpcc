#!/usr/bin/env sysbench

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

-- ----------------------------------------------------------------------
-- TPCC-like workload
-- ----------------------------------------------------------------------

require("tpcc_common")
require("tpcc_run")
require("tpcc_check")

function thread_init()

   drv = sysbench.sql.driver()
   con = drv:connect()

   set_isolation_level(drv,con) 

   if drv:name() == "mysql" then 
     con:query("SET autocommit=0")
   end
   
end

function event()
  -- print( NURand (1023,1,3000))
  local max_trx =  sysbench.opt.enable_purge == "yes" and 24 or 23
  local trx_type = sysbench.rand.uniform(1,max_trx)
  if trx_type <= 10 then
    trx="new_order"
  elseif trx_type <= 20 then
    trx="payment"
  elseif trx_type <= 21 then
    trx="orderstatus"
  elseif trx_type <= 22 then
    trx="delivery"
  elseif trx_type <= 23 then
    trx="stocklevel"
  elseif trx_type <= 24 then
    trx="purge"
  end

-- Execute transaction
   _G[trx]()

end

function sysbench.hooks.before_restart_event(err)
  con:query("ROLLBACK")
end

function sysbench.hooks.report_intermediate(stat)
-- --   print("my stat: ", val)
   local recent_notpm
   local last_maxid
   if not con then
	thread_init()
   end
   recent_notpm, last_maxid = con:query_row("select count(id), max(id) from notpm where done = 0")
   if not last_maxid then
	last_maxid = con:query_row("select count(id), max(id) from notpm")
   end
   con:query(([[update notpm set done = 1 where id <= %d]]):format(last_maxid))
   con:query("commit")
   time = os.date("*t")
   print(("%02d:%02d:%02d"):format(time.hour, time.min, time.sec) .. " Number of New Order Transactions: " .. recent_notpm)
   --if  sysbench.opt.report_csv == "yes" then
   --	sysbench.report_csv(stat)
   --else
   --	sysbench.report_default(stat)
   --end
end

function sysbench.hooks.report_cumulative(stat)
   local total_notpm
   local avg_notpm
   if not con then
	thread_init()
   end
   total_notpm = con:query_row("select count(id) from notpm")

   avg_notpm = total_notpm/sysbench.opt.time*60
   print(total_notpm .. " new order transactions done during " .. sysbench.opt.time .. " second for an average of: " .. avg_notpm .. " transactions per minute (NOTPM)")
   con:query("truncate notpm")
end


-- vim:ts=4 ss=4 sw=4 expandtab
