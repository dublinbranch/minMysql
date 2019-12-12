#pragma once

#include <unordered_map>

// Forward declaration
template <typename T>
class mi_tls_repository;

template <typename T>
class mi_tls : protected mi_tls_repository<T> {
      public:
	mi_tls<T>() {
	}

	mi_tls<T>(const T& value) {
		this->store(reinterpret_cast<uintptr_t>(this), value);
	}

	mi_tls<T>& operator=(const T& value) {
		this->store(reinterpret_cast<uintptr_t>(this), value);
		return *this;
	}

	operator T() {
		return this->load(reinterpret_cast<uintptr_t>(this));
	}

	~mi_tls() {
		this->remove(reinterpret_cast<uintptr_t>(this));
	}
};

template <typename T>
class mi_tls_repository {
      protected:
	void store(uintptr_t instance, T value) {
		repository->operator[](instance) = value;
	}

	T load(uintptr_t instance) {
		return repository->operator[](instance);
	}

	void remove(uintptr_t instance) {
		if (!repository) {
			//already destructed because empty ?
			return;
		}
		if (repository->find(instance) != repository->cend()) {
			repository->erase(instance);
		}
	}

	mi_tls_repository() {
		copyCounter++;
		if (!repository) {
			repository = new std::unordered_map<uintptr_t, T>();
		}
	}

	~mi_tls_repository() {
		copyCounter--;
		if (copyCounter == 0) {
			delete (repository);
			repository = nullptr;
		}
	}

      private:
	//To avoid problem of this beeing deallocated prematurely, just use a ptr, that will be inited just once
	inline static int                                            copyCounter = 0;
	inline static thread_local std::unordered_map<uintptr_t, T>* repository  = nullptr;
};
