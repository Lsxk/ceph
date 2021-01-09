//
// Created by juzhou.wjz on 2021/1/7.
//
#include "svc_sys_obj_core_mongo.h"

#define dout_subsys ceph_subsys_rgw

// TODO
int RGWSI_SysObj_Core::get_rados_obj(RGWSI_Zone *zone_svc,
                                     const rgw_raw_obj& obj,
                                     RGWSI_RADOS::Obj *pobj)
{
    if (obj.oid.empty()) {
        ldout(rados_svc->ctx(), 0) << "ERROR: obj.oid is empty" << dendl;
        return -EINVAL;
    }

    *pobj = std::move(rados_svc->obj(obj));
    int r = pobj->open();
    if (r < 0) {
        return r;
    }

    return 0;
}

// TODO
int RGWSI_SysObj_Core::get_system_obj_state_impl(RGWSysObjectCtxBase *rctx, const rgw_raw_obj& obj, RGWSysObjState **state, RGWObjVersionTracker *objv_tracker)
{
    if (obj.empty()) {
        return -EINVAL;
    }

    RGWSysObjState *s = rctx->get_state(obj);
    ldout(cct, 20) << "get_system_obj_state: rctx=" << (void *)rctx << " obj=" << obj << " state=" << (void *)s << " s->prefetch_data=" << s->prefetch_data << dendl;
    *state = s;
    if (s->has_attrs) {
        return 0;
    }

    s->obj = obj;

    int r = raw_stat(obj, &s->size, &s->mtime, &s->epoch, &s->attrset, (s->prefetch_data ? &s->data : nullptr), objv_tracker);
    if (r == -ENOENT) {
        s->exists = false;
        s->has_attrs = true;
        s->mtime = real_time();
        return 0;
    }
    if (r < 0)
        return r;

    s->exists = true;
    s->has_attrs = true;
    s->obj_tag = s->attrset[RGW_ATTR_ID_TAG];

    if (s->obj_tag.length())
        ldout(cct, 20) << "get_system_obj_state: setting s->obj_tag to "
                       << s->obj_tag.c_str() << dendl;
    else
        ldout(cct, 20) << "get_system_obj_state: s->obj_tag was set empty" << dendl;

    return 0;
}

int RGWSI_SysObj_Core::write_data(const rgw_raw_obj& obj,
                                  const bufferlist& bl,
                                  bool exclusive,
                                  RGWObjVersionTracker *objv_tracker)
{
    RGWSI_RADOS::Obj rados_obj;
    int r = get_rados_obj(zone_svc, obj, &rados_obj);
    if (r < 0) {
        ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
        return r;
    }

    librados::ObjectWriteOperation op;

    if (exclusive) {
        op.create(true);
    }

    if (objv_tracker) {
        objv_tracker->prepare_op_for_write(&op);
    }
    op.write_full(bl);
    r = rados_obj.operate(&op, null_yield);
    if (r < 0)
        return r;

    if (objv_tracker) {
        objv_tracker->apply_write();
    }
    return 0;
}