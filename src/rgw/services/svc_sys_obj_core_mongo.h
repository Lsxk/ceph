//
// Created by juzhou.wjz on 2021/1/7.
//

#ifndef CEPH_SVC_SYS_OBJ_CORE_MONGO_H
#define CEPH_SVC_SYS_OBJ_CORE_MONGO_H

#endif //CEPH_SVC_SYS_OBJ_CORE_MONGO_H

#include "svc_sys_obj_core.h"

class RGWSI_SysObj_Core_Mongo : public RGWSI_SysObj_Core {
    friend class RGWServices_Def;

    friend class RGWSI_SysObj;

protected:
    RGWSI_RADOS *rados_svc{nullptr};
    RGWSI_Zone *zone_svc{nullptr};

    struct GetObjState {
        RGWSI_RADOS::Obj rados_obj;
        bool has_rados_obj{false};
        uint64_t last_ver{0};

        GetObjState() {}

        int get_rados_obj(RGWSI_RADOS *rados_svc,
                          RGWSI_Zone *zone_svc,
                          const rgw_raw_obj &obj,
                          RGWSI_RADOS::Obj **pobj);
    };


    void core_init(RGWSI_RADOS *_rados_svc,
                   RGWSI_Zone *_zone_svc) {
        rados_svc = _rados_svc;
        zone_svc = _zone_svc;
    }

    int get_rados_obj(RGWSI_Zone *zone_svc, const rgw_raw_obj &obj, RGWSI_RADOS::Obj *pobj);

    virtual int raw_stat(const rgw_raw_obj &obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
                         map<string, bufferlist> *attrs, bufferlist *first_chunk,
                         RGWObjVersionTracker *objv_tracker);

    virtual int read(RGWSysObjectCtxBase &obj_ctx,
                     GetObjState &read_state,
                     RGWObjVersionTracker *objv_tracker,
                     const rgw_raw_obj &obj,
                     bufferlist *bl, off_t ofs, off_t end,
                     map<string, bufferlist> *attrs,
                     bool raw_attrs,
                     rgw_cache_entry_info *cache_info,
                     boost::optional<obj_version>);

    virtual int remove(RGWSysObjectCtxBase &obj_ctx,
                       RGWObjVersionTracker *objv_tracker,
                       const rgw_raw_obj &obj);

    virtual int write(const rgw_raw_obj &obj,
                      real_time *pmtime,
                      map<std::string, bufferlist> &attrs,
                      bool exclusive,
                      const bufferlist &data,
                      RGWObjVersionTracker *objv_tracker,
                      real_time set_mtime);

    virtual int write_data(const rgw_raw_obj &obj,
                           const bufferlist &bl,
                           bool exclusive,
                           RGWObjVersionTracker *objv_tracker);

    virtual int get_attr(const rgw_raw_obj &obj, const char *name, bufferlist *dest);

    virtual int set_attrs(const rgw_raw_obj &obj,
                          map<string, bufferlist> &attrs,
                          map<string, bufferlist> *rmattrs,
                          RGWObjVersionTracker *objv_tracker);

    virtual int omap_get_all(const rgw_raw_obj &obj, std::map<string, bufferlist> *m);

    virtual int omap_get_vals(const rgw_raw_obj &obj,
                              const string &marker,
                              uint64_t count,
                              std::map<string, bufferlist> *m,
                              bool *pmore);

    virtual int omap_set(const rgw_raw_obj &obj, const std::string &key, bufferlist &bl, bool must_exist = false);

    virtual int omap_set(const rgw_raw_obj &obj, const map<std::string, bufferlist> &m, bool must_exist = false);

    virtual int omap_del(const rgw_raw_obj &obj, const std::string &key);

    virtual int notify(const rgw_raw_obj &obj,
                       bufferlist &bl,
                       uint64_t timeout_ms,
                       bufferlist *pbl);

    /* wrappers */
    int get_system_obj_state_impl(RGWSysObjectCtxBase *rctx, const rgw_raw_obj &obj, RGWSysObjState **state,
                                  RGWObjVersionTracker *objv_tracker);

    int get_system_obj_state(RGWSysObjectCtxBase *rctx, const rgw_raw_obj &obj, RGWSysObjState **state,
                             RGWObjVersionTracker *objv_tracker);

    int stat(RGWSysObjectCtxBase &obj_ctx,
             GetObjState &state,
             const rgw_raw_obj &obj,
             map<string, bufferlist> *attrs,
             bool raw_attrs,
             real_time *lastmod,
             uint64_t *obj_size,
             RGWObjVersionTracker *objv_tracker);

public:
    RGWSI_SysObj_Core(CephContext
    *cct):
    RGWServiceInstance(cct) {}

    RGWSI_Zone *get_zone_svc() {
        return zone_svc;
    }
};
