#ifndef SIMPLESELECT_H
#define SIMPLESELECT_H

#include "Select.h"

// the "filtering" transform
template <typename InputT>
class SimpleSelect : public Select<InputT> {
  using RecordT = Record<InputT>;
public:
  SimpleSelect(string name = "simple_select") : Select<InputT>(name) { }

  // being static, there's not polymorphsim cost.
  inline static bool do_select(RecordT const & rec) {
    if (rec.data < 2000)
      return false;
    else
      return true;
  }
};

#endif /* SIMPLESELECT_H */
