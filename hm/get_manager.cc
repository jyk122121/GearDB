#include "../hm/get_manager.h"
#include "../hm/hm_manager.h"
#include "../include/leveldb/options.h"
#include "../hm/my_log.h"

namespace leveldb{
    Singleton::Singleton() {}

    Singleton::~Singleton() {}

    HMManager* Singleton::Gethmmanager() {
        int reset = 0; // use_exising_db == true -> reset = 0
        if(hm_manager_ == NULL){
            hm_manager_ = new HMManager(Options().comparator, reset);
        }
        return hm_manager_;
    }

    HMManager* Singleton::hm_manager_ = NULL;

    Singleton::Deletor::~Deletor() {
        if(hm_manager_ != NULL) delete hm_manager_;
    }

    Singleton::Deletor Singleton::deletor_;
}
