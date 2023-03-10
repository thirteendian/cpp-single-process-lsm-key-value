//
// Created by zxjcarrot on 2019-07-24.
//

#ifndef SILKSTORE_ITER_H
#define SILKSTORE_ITER_H

#include <stdint.h>
#include "leveldb/db.h"
#include "db/dbformat.h"

namespace leveldb {
namespace silkstore {
// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter : public Iterator {
public:
    // Which direction is the iterator currently moving?
    // (1) When moving forward, the internal iterator is positioned at
    //     the exact entry that yields this->key(), this->value()
    // (2) When moving backwards, the internal iterator is positioned
    //     just before all entries whose user key == this->key().
    enum Direction {
        kForward,
        kReverse
    };

    DBIter(const Comparator *cmp, Iterator *iter, SequenceNumber s)
            : user_comparator_(cmp),
              iter_(iter),
              sequence_(s),
              direction_(kForward),
              valid_(false) {
           // printf("DBIter : public Iterator in silkstore\n ");
    }

    virtual ~DBIter() {
        delete iter_;
    }

    virtual bool Valid() const { return valid_; }

    virtual Slice key() const {
        assert(valid_);
        return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
    }

    Slice internal_key() const {
        assert(direction_ == kForward); // only works for forward iteration
        return (direction_ == kForward) ? iter_->key() : saved_key_;
    }

    virtual Slice value() const {
        assert(valid_);
        return (direction_ == kForward) ? iter_->value() : saved_value_;
    }

    virtual Status status() const {
        if (status_.ok()) {
            return iter_->status();
        } else {
            return status_;
        }
    }

    virtual void Next() {
        assert(valid_);

        if (direction_ == kReverse) {  // Switch directions?
            direction_ = kForward;
            // iter_ is pointing just before the entries for this->key(),
            // so advance into the range of entries for this->key() and then
            // use the normal skipping code below.
            if (!iter_->Valid()) {
                iter_->SeekToFirst();
            } else {
                iter_->Next();
            }
            if (!iter_->Valid()) {
                valid_ = false;
                saved_key_.clear();
                return;
            }
            // saved_key_ already contains the key to skip past.
        } else {
            // Store in saved_key_ the current key so we skip it below.
            SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
        }

        FindNextUserEntry(true, &saved_key_);
    }

    virtual void Prev() {
        assert(valid_);

        if (direction_ == kForward) {  // Switch directions?
            // iter_ is pointing at the current entry.  Scan backwards until
            // the key changes so we can use the normal reverse scanning code.
            assert(iter_->Valid());  // Otherwise valid_ would have been false
            SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
            while (true) {
                iter_->Prev();
                if (!iter_->Valid()) {
                    valid_ = false;
                    saved_key_.clear();
                    ClearSavedValue();
                    return;
                }
                if (user_comparator_->Compare(ExtractUserKey(iter_->key()),
                                              saved_key_) < 0) {
                    break;
                }
            }
            direction_ = kReverse;
        }

        FindPrevUserEntry();
    }

    virtual void Seek(const Slice &target) {
        direction_ = kForward;
        ClearSavedValue();
        saved_key_.clear();
        AppendInternalKey(
                &saved_key_, ParsedInternalKey(target, sequence_, kValueTypeForSeek));
        iter_->Seek(saved_key_);
        if (iter_->Valid()) {
            FindNextUserEntry(false, &saved_key_ /* temporary storage */);
        } else {
            valid_ = false;
        }
    }

    virtual void SeekToFirst() {
        direction_ = kForward;
        ClearSavedValue();        
        iter_->SeekToFirst();
        if (iter_->Valid()) {
            FindNextUserEntry(false, &saved_key_ /* temporary storage */);
        } else {
            printf("SeekToFirst invalid\n ");
            
            valid_ = false;
        }
    }

    virtual void SeekToLast() {
        direction_ = kReverse;
        ClearSavedValue();
        iter_->SeekToLast();
        FindPrevUserEntry();
    }


private:
    void FindNextUserEntry(bool skipping, std::string *skip) {
        // Loop until we hit an acceptable entry to yield
        assert(iter_->Valid());
        assert(direction_ == kForward);
        do {
            ParsedInternalKey ikey;
            if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
                switch (ikey.type) {
                    case kTypeDeletion:
                        // Arrange to skip all upcoming entries for this key since
                        // they are hidden by this deletion.
                        SaveKey(ikey.user_key, skip);
                        skipping = true;
                        break;
                    case kTypeValue:
                        if (skipping &&
                            user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
                            // Entry hidden
                        } else {
                            valid_ = true;
                            saved_key_.clear();
                            return;
                        }
                        break;
                }
            }
            iter_->Next();
        } while (iter_->Valid());
        saved_key_.clear();
        valid_ = false;
    }

    void FindPrevUserEntry() {
        assert(direction_ == kReverse);

        ValueType value_type = kTypeDeletion;
        if (iter_->Valid()) {
            do {
                ParsedInternalKey ikey;
                if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
                    if ((value_type != kTypeDeletion) &&
                        user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
                        // We encountered a non-deleted value in entries for previous keys,
                        break;
                    }
                    value_type = ikey.type;
                    if (value_type == kTypeDeletion) {
                        saved_key_.clear();
                        ClearSavedValue();
                    } else {
                        Slice raw_value = iter_->value();
                        if (saved_value_.capacity() > raw_value.size() + 1048576) {
                            std::string empty;
                            swap(empty, saved_value_);
                        }
                        SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
                        saved_value_.assign(raw_value.data(), raw_value.size());
                    }
                }
                iter_->Prev();
            } while (iter_->Valid());
        }

        if (value_type == kTypeDeletion) {
            // End
            valid_ = false;
            saved_key_.clear();
            ClearSavedValue();
            direction_ = kForward;
        } else {
            valid_ = true;
        }
    }

    bool ParseKey(ParsedInternalKey *ikey) {
        Slice k = iter_->key();
        //printf("ParseKey: %s ", iter_->key().data());
        if (!ParseInternalKey(k, ikey)) {
            status_ = Status::Corruption("corrupted internal key in DBIter");
            return false;
        } else {
            return true;
        }
    }


    inline void SaveKey(const Slice &k, std::string *dst) {
        dst->assign(k.data(), k.size());
    }

    inline void ClearSavedValue() {
        if (saved_value_.capacity() > 1048576) {
            std::string empty;
            swap(empty, saved_value_);
        } else {
            saved_value_.clear();
        }
    }

    const Comparator *const user_comparator_;
    Iterator *const iter_;
    SequenceNumber const sequence_;

    Status status_;
    std::string saved_key_;     // == current key when direction_==kReverse
    std::string saved_value_;   // == current raw value when direction_==kReverse
    Direction direction_;
    bool valid_;

    // No copying allowed
    DBIter(const DBIter &);

    void operator=(const DBIter &);
};

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
Iterator *NewDBIterator(
        const Comparator *user_key_comparator,
        Iterator *internal_iter,
        SequenceNumber sequence);

}  // namespace silkstore
} // namespace leveldb

#endif //SILKSTORE_ITER_H
