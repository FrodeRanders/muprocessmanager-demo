/*
 * Copyright (C) 2017 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package demo;

import org.gautelis.muprocessmanager.MuActivity;
import org.gautelis.muprocessmanager.MuActivityParameters;

public class FirstActivity implements MuActivity {

    private static final double forwardFailureProbability = 0.01;
    private static final double backwardFailureProbability = 0.01;

    public FirstActivity() {}

    @Override
    public boolean forward(MuActivityParameters args) {
        return !(Math.random() < forwardFailureProbability);
    }

    @Override
    public boolean backward(MuActivityParameters args) {
        return !(Math.random() < backwardFailureProbability);
    }
}