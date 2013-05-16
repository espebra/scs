local M = {}

local common = require "scs.common"
local ngx = ngx

function M.initiate_periodic_health_checks(delay)
    if ngx.shared.health_check_timer then
        return
    else
        ngx.shared.health_check_timer = true
    end

    local handler
    handler = function (premature)
        -- do some routine job in Lua just like a cron job
        if premature then
            return
        end

        local sites = common.get_all_sites()
        while true do
            common.update_status_for_all_hosts(sites)
            ngx.sleep(delay)
        end
        return
    end

    local ok, err = ngx.timer.at(0, handler)
    if not ok then
        ngx.log(ngx.ERR, "Failed to create the health check timer: " .. err)
        return
    end
end

function M.initiate_batch_synchronization(delay)
    if ngx.shared.synchronization_timer then
        return
    else
        ngx.shared.synchronization_timer = true
    end

    local handler
    handler = function (premature)
        -- do some routine job in Lua just like a cron job
        if premature then
            return
        end

        local path = common.get_storage_directory()
        if not common.is_file(path) then
            ngx.log(ngx.ERR,"Unable to start the sync prosess!" .. path .. " does not exist")
            return
        end

        while true do
            local start = ngx.time()
            local versions = {}

            ngx.log(ngx.ERR,"Batch job started")

            local entry, versions, popen = nil, {}, io.popen
            for entry in popen('find ' .. path .. ' -type f -printf "%T@\t%s\t%f\t%h\n" | sort -nr'):lines() do
                local m, err = ngx.re.match(entry, "^([0-9]+)[^\t]+\t([0-9]+)\t([0-9]+)-([a-f0-9]+).([a-z]+)\t" .. path .. "/([^/]+).*/([^/]+)$","j")
                if m then
                    if #m == 7 then
                        local mtime = tonumber(m[1])
                        local size = tonumber(m[2])
                        local version = tonumber(m[3])
                        local md5 = m[4]
                        local filetype = m[5]
                        local bucket = m[6]
                        local object_base64 = m[7]
                        local object = ngx.decode_base64(object_base64)

                        ngx.log(ngx.ERR,"mtime: " .. m[1] .. ", size: " .. m[2] .. ", version: " .. m[3] .. ", md5: " .. m[4] .. ", type: " .. m[5] .. ", bucket: " .. m[6])

                        if filetype == "data" then
                            local valid = common.is_checksum_valid(bucket, object, version, md5)
                            if valid then
                                ngx.log(ngx.ERR,"Object " .. bucket .. "/" .. object .. " version " .. version .. " is valid")
                                -- Replicate to other hosts.
                            else
                                ngx.log(ngx.ERR,"Object " .. bucket .. "/" .. object .. " version " .. version .. " is corrupt")
                                -- common.quarantine(bucket, object, version, md5)
    
                            end
                        elseif filetype == "ts" then
                            -- Remove old versions if tombstone exists.
                        end
                    end
                end
            end
            
            local elapsed = ngx.now() - start
            ngx.log(ngx.ERR,"Batch job completed in " .. elapsed .. " seconds.")
            ngx.sleep(delay)
        end
    end

    local ok, err = ngx.timer.at(0, handler)
    if not ok then
        ngx.log(ngx.ERR, "Failed to create the synchronization timer: " .. err)
        return
    end
end

return M
