#ifndef METRICITY_REGEX_HELPER_H
#define METRICITY_REGEX_HELPER_H

#include <regex.h>
#include <stdalign.h>
#include <stddef.h>
#include <string.h>

typedef struct {
    alignas(regex_t) unsigned char storage[sizeof(regex_t)];
} metricity_regex_t;

static inline void metricity_regex_init(metricity_regex_t *wrapper) {
    memset(wrapper->storage, 0, sizeof(wrapper->storage));
}

static inline regex_t *metricity_regex_ptr(metricity_regex_t *wrapper) {
    return (regex_t *)(void *)wrapper->storage;
}

static inline const regex_t *metricity_regex_ptr_const(const metricity_regex_t *wrapper) {
    return (const regex_t *)(const void *)wrapper->storage;
}

#endif // METRICITY_REGEX_HELPER_H
