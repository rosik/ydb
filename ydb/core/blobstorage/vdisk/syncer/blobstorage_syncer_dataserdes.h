#pragma once

#include "defs.h"
#include "blobstorage_syncer_data.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TSyncNeighbors::TOldSer
    ////////////////////////////////////////////////////////////////////////////
    // TODO: remove it after migration to the new format
    class TSyncNeighbors::TOldSer {
    public:
        TOldSer(IOutputStream &str, const TBlobStorageGroupInfo *info)
            : Str(str)
            , Info(info)
        {}

        void operator() (const TValue &val) {
            TVDiskID vd = Info->GetVDiskId(val.OrderNumber);
            vd.Serialize(Str);
            val.Get().Serialize(Str);
        }

        void Finish() {}

    private:
        IOutputStream &Str;
        const TBlobStorageGroupInfo *Info;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSyncNeighbors::TOldDes
    ////////////////////////////////////////////////////////////////////////////
    class TSyncNeighbors::TOldDes {
    public:
        TOldDes(const TString& logPrefix, IInputStream &str)
            : VDiskLogPrefix(logPrefix)
            , Str(str)
        {}

        void operator() (TValue &val) {
            TVDiskID vdisk(Str);
            GroupId = vdisk.GroupID;
            GroupGeneration = vdisk.GroupGeneration;
            Y_VERIFY_S(val.VDiskIdShort == vdisk, VDiskLogPrefix <<
                    "val.VDiskId# " << val.VDiskIdShort.ToString() << " vdisk# " << vdisk.ToString());
            val.Get().ParseFromArcadiaStream(Str);
        }

        void Finish() {
            char c = '\0';
            if (Str.ReadChar(c))
                ythrow yexception() << "not eof";
        }

        TGroupId GetGroupId() const { return GroupId; }
        ui32 GetGroupGeneration() const { return GroupGeneration; }

    private:
        const TString VDiskLogPrefix;
        IInputStream &Str;
        TGroupId GroupId = TGroupId::Zero();
        ui32 GroupGeneration = 0;
    };


    ////////////////////////////////////////////////////////////////////////////
    // TSyncNeighbors::TSer
    ////////////////////////////////////////////////////////////////////////////
    class TSyncNeighbors::TSer {
    public:
        TSer(IOutputStream &str, const TBlobStorageGroupInfo *info)
            : LocalProto()
            , Proto(&LocalProto)
            , Str(&str)
            , GroupId(info->GroupID)
            , GroupGeneration(info->GroupGeneration)
        {}

        TSer(IOutputStream &str, TGroupId groupId, ui32 groupGen)
            : LocalProto()
            , Proto(&LocalProto)
            , Str(&str)
            , GroupId(groupId)
            , GroupGeneration(groupGen)
        {}

        TSer(NKikimrVDiskData::TSyncerEntryPoint *pb, const TBlobStorageGroupInfo *info)
            : LocalProto()
            , Proto(pb)
            , Str(nullptr)
            , GroupId(info->GroupID)
            , GroupGeneration(info->GroupGeneration)
        {}

        void operator() (const TValue &val) {
            TVDiskID vd = TVDiskID(GroupId, GroupGeneration, val.VDiskIdShort);
            auto item = Proto->AddEntries();
            VDiskIDFromVDiskID(vd, item->MutableVDiskID());
            val.Get().Serialize(*item);
        }

        void Finish() {
            if (Str)
                Proto->SerializeToArcadiaStream(Str);
        }

    private:
        NKikimrVDiskData::TSyncerEntryPoint LocalProto;
        NKikimrVDiskData::TSyncerEntryPoint *Proto = nullptr;
        IOutputStream *Str = nullptr;
        TGroupId GroupId = TGroupId::Zero();
        ui32 GroupGeneration = 0;
    };


    ////////////////////////////////////////////////////////////////////////////
    // TSyncNeighbors::TDes
    ////////////////////////////////////////////////////////////////////////////
    class TSyncNeighbors::TDes {
    public:
        TDes(const TString logPrefix, IInputStream &str)
            : VDiskLogPrefix(logPrefix)
            , Proto(&LocalProto)
        {
            auto res = LocalProto.ParseFromArcadiaStream(&str);
            if (!res)
                ythrow yexception() << "NKikimrVDiskData::TSyncerNeighbors parse error";
        }

        TDes(const TString logPrefix, const NKikimrVDiskData::TSyncerEntryPoint *pb)
            : VDiskLogPrefix(logPrefix)
            , Proto(pb)
        {}

        void operator() (TValue &val) {
            const auto &item = Proto->GetEntries(Counter);
            ++Counter;
            TVDiskID vdisk = VDiskIDFromVDiskID(item.GetVDiskID());
            Y_VERIFY_S(val.VDiskIdShort == TVDiskIdShort(vdisk), VDiskLogPrefix <<
                     "val.VDiskId# " << val.VDiskIdShort.ToString() << " vdisk# " << vdisk.ToString());
            val.Get().Parse(item);
        }

        void Finish() {
            Y_VERIFY_S(Counter == Proto->EntriesSize(), VDiskLogPrefix <<
                    "Counter# " << Counter << " size# " << unsigned(Proto->EntriesSize()));
        }

    private:
        const TString VDiskLogPrefix;
        NKikimrVDiskData::TSyncerEntryPoint LocalProto;
        const NKikimrVDiskData::TSyncerEntryPoint *Proto = nullptr;
        unsigned Counter = 0;
    };

} // NKikimr
