//
// Created by juzhou.wjz on 2021/1/7.
//
#include "svc_sys_obj_core_mongo.h"

#define dout_subsys ceph_subsys_rgw

/**
 * stat operator, return obj size and last modify time
 * The result will be ENOENT if the object doesn't exist.  If it does
 * exist and no other error occurs the server returns the size and last
 * modification time of the target object as output data (in little
 * endian format).  The size is a 64 bit unsigned and the time is
 * ceph_timespec structure (two unsigned 32-bit integers, representing
 * a seconds and nanoseconds value).
 * @param obj
 * @param psize
 * @param pmtime
 * @param epoch
 * @param attrs
 * @param first_chunk
 * @param objv_tracker
 * @return
 */
int RGWSI_SysObj_Core::raw_stat(const rgw_raw_obj &obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
                                map<string, bufferlist> *attrs, bufferlist *first_chunk,
                                RGWObjVersionTracker *objv_tracker) {

    RGWSI_RADOS::Obj rados_obj;
    int r = get_rados_obj(zone_svc, obj, &rados_obj);
    if (r < 0) {
        return r;
    }

    uint64_t size = 0;
    struct timespec mtime_ts;

    librados::ObjectReadOperation op;
    if (objv_tracker) {
        objv_tracker->prepare_op_for_read(&op);
    }
    op.getxattrs(attrs, nullptr);
    if (psize || pmtime) {

        // todo read size, modify time
        rgw_pool pool =  obj.pool;
        string coll_name = pool.name + pool.ns;

        string key = obj.oid;

/*        bsoncxx::stdx::optional<bsoncxx::document::value> obj_mongo = mongo_client["test"][coll_name].find_one({"key": key});


        if (obj_mongo) {
            std:cout << bsoncxx::to_json(*obj_mongo) << "\n";
        }

        op.stat2(&size, &mtime_ts, nullptr);*/
    }
    if (first_chunk) {
        // todo read first_chunk
        op.read(0, cct->_conf->rgw_max_chunk_size, first_chunk, nullptr);
    }
    bufferlist outbl;
    r = rados_obj.operate(&op, &outbl, null_yield);

    if (epoch) {
        *epoch = rados_obj.get_last_version();
    }

    if (r < 0)
        return r;

    if (psize)
        *psize = size;
    if (pmtime)
        *pmtime = ceph::real_clock::from_timespec(mtime_ts);

    return 0;
}